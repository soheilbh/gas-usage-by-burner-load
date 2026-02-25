"""
One-off script: for a given period, run the pipeline (no UI), then print
total estimated gas (K × burner_load) vs total actual gas from InfluxDB.
Delete this file after use.

Usage (from repo root):
  python check_est_vs_actual.py
  # or with custom range:
  python check_est_vs_actual.py --from 2024-02-05 --to 2024-06-05
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

if __name__ == "__main__":
    _root = os.path.dirname(os.path.abspath(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from config import InfluxConfig
from app_settings import DEFAULT_K
from full_cleaning_pipeline import run_pipeline


# Time periods for multi-period % error comparison (1 month, 3 months, 5 months, 7 months)
MULTI_PERIODS = [
    ("2024-01-01", "2024-01-31", "1 mo"),
    ("2024-03-01", "2024-03-31", "1 mo"),
    ("2024-05-01", "2024-05-31", "1 mo"),
    ("2024-01-01", "2024-03-31", "3 mo"),
    ("2024-02-01", "2024-06-30", "5 mo"),
    ("2024-01-01", "2024-07-31", "7 mo"),
]


def run_one_period(config: InfluxConfig, start: datetime, end: datetime, k: float, all_hours: bool):
    """Return (total_measured, total_estimated, n_hours) or (None, None, 0) on failure."""
    hourly = run_pipeline(config, start, end, only_100pct=not all_hours)
    if hourly is None or hourly.empty or "gas" not in hourly.columns:
        return None, None, 0
    gas_ok = hourly["gas"].notna()
    total_measured = hourly["gas"].sum()
    load = hourly["burner_load"].astype(float)
    if "operational_minutes" in hourly.columns:
        op_min = hourly["operational_minutes"].fillna(0).clip(0, 60)
        est = load * k * (op_min / 60.0)
    else:
        est = load * k
    total_estimated = est[gas_ok].sum()
    total_measured_fair = hourly.loc[gas_ok, "gas"].sum()
    return total_measured_fair, total_estimated, len(hourly)


def main():
    parser = argparse.ArgumentParser(description="Compare total estimated vs actual gas for a period.")
    parser.add_argument("--from", dest="from_", type=str, default="2024-02-05", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", type=str, default="2024-06-05", help="End date YYYY-MM-DD")
    parser.add_argument("-k", type=float, default=DEFAULT_K, help="K (default from app_settings)")
    parser.add_argument("--all-hours", action="store_true", help="Include all hours (no 100%% op filter); compare total est vs total actual")
    parser.add_argument("--multi", action="store_true", help="Run 3 different periods and print %% error comparison (uses all-hours + op_min scaling)")
    args = parser.parse_args()

    config = InfluxConfig.from_env()

    if args.multi:
        print(f"K = {args.k}  |  gas_est = K×load×(op_min/60)  |  total gas = sum over period (no filter)\n")
        print("Span   Period              |  Total measured (m³)  |  Total estimated (m³)  |  Δ (est−meas)  |  % error")
        print("-" * 105)
        for from_, to_, label in MULTI_PERIODS:
            start = datetime.strptime(from_, "%Y-%m-%d")
            end = datetime.strptime(to_, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
            meas, est, n = run_one_period(config, start, end, args.k, all_hours=True)
            if meas is None or meas == 0:
                print(f"  {label:4}  {from_} to {to_}  |  (no data)")
                continue
            diff = est - meas
            pct = (diff / meas) * 100.0
            print(f"  {label:4}  {from_} to {to_}  |  {meas:>12,.0f}  |  {est:>12,.0f}  |  {diff:>+10,.0f}  |  {pct:>+5.2f}%")
        print()
        return

    start = datetime.strptime(args.from_, "%Y-%m-%d")
    end = datetime.strptime(args.to, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
    if start >= end:
        print("Error: --from must be before --to")
        sys.exit(1)

    only_100 = not args.all_hours
    print(f"Querying InfluxDB: {args.from_} to {args.to} (K = {args.k}) ...")
    if only_100:
        print("  Filter: 100% operational hours only (60 min s_run+fan1+fan2).")
    else:
        print("  No filter: all hours (including partial, e.g. 30 min run).")
    hourly = run_pipeline(config, start, end, only_100pct=only_100)
    if hourly is None or hourly.empty:
        print("No data returned. Check InfluxDB and time range.")
        sys.exit(1)

    if "gas" not in hourly.columns:
        print("No measured gas in pipeline output. Cannot compare.")
        sys.exit(1)

    gas_ok = hourly["gas"].notna()
    n_with_gas = int(gas_ok.sum())
    n_nan = int(hourly["gas"].isna().sum())
    n_hours = len(hourly)
    assert n_nan + n_with_gas == n_hours, "sanity check"
    total_measured = hourly["gas"].sum()
    load = hourly["burner_load"].astype(float)
    # Scale by (op_min/60) when available (same as app: partial hours count proportionally)
    if "operational_minutes" in hourly.columns:
        op_min = hourly["operational_minutes"].fillna(0).clip(0, 60)
        est = load * args.k * (op_min / 60.0)
    else:
        est = load * args.k
    total_estimated = est.sum()
    diff = total_estimated - total_measured
    # Fair comparison: only hours where we have both (so difference is not from missing gas)
    est_where_meas = est[gas_ok].sum()
    meas_where_meas = hourly.loc[gas_ok, "gas"].sum()
    diff_fair = est_where_meas - meas_where_meas

    print()
    if only_100:
        print("--- Est. vs actual (100% operational hours only) ---")
    else:
        print("--- Est. vs actual (ALL hours, gas_est = K×load×(op_min/60)) ---")
    print(f"  Total measured (actual):  {total_measured:,.1f} m³")
    print(f"  Total estimated (K×load):  {total_estimated:,.1f} m³")
    print(f"  Difference (est − meas):   {diff:+,.1f} m³")
    print(f"  Hours:                    {n_hours}  (with gas: {n_with_gas}, missing gas: {n_nan})")
    if n_with_gas < n_hours:
        print(f"  [Fair: only {n_with_gas}h with gas] est {est_where_meas:,.1f} vs meas {meas_where_meas:,.1f} → Δ {diff_fair:+,.1f} m³")
    else:
        print(f"  [Same hours] est vs meas on same hours → Δ {diff_fair:+,.1f} m³")
    print()
    if only_100:
        print("Why a difference? Possible causes:")
        print("  • K was fitted on another period; load–gas relation can vary by period.")
        print("  • Model is gas = K×load (no intercept); real burners often have standby/pilot.")
        print("  • If 'with gas' < hours, some hours lack gas data (reindex alignment).")
    else:
        print("(No operational filter: includes partial hours. If total est ≈ total actual, estimation is good.)")
    print()


if __name__ == "__main__":
    main()
