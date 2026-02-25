"""
Calibration pipeline: uses full_cleaning_pipeline (fetch + burner 4-level + 100% hours), then fit K.
Used by app "Calibrate k".
"""
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from config import InfluxConfig
from full_cleaning_pipeline import run_pipeline
from influx_queries import fetch_all_series, query_energy_gas_raw
from processing import calibrate_k, filter_working_time, hourly_for_calibration


def run_find_k(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    raw_interval: str = "1m",
    only_100pct_operational: bool = False,
    use_burner_cleaning: bool = True,
    match_reference_pipeline: bool = True,
) -> Tuple[Optional[float], Optional[dict], Optional[pd.DataFrame], Optional[pd.Series]]:
    """
    Fetch from InfluxDB, clean burner module, build hourly load, fit K.
    Returns (k, metrics, hourly_df, gas_hourly) or (None, None, None, None).

    match_reference_pipeline: if True (default), use full_cleaning_pipeline (same as app):
      100% = 60 operational minutes; burner 4-level cleaning; gas aligned to hourly.
    use_burner_cleaning: no-op when using pipeline path; fallback path uses raw burner_load (no separate cleaning).
    only_100pct_operational: keep only hours with 60 operational minutes.
    """
    # Calibration: same pipeline as main app (s_run, fan1, fan2, burner module).
    if match_reference_pipeline and only_100pct_operational:
        df = run_pipeline(config, start, end)
        if df is None or df.empty or "burner_load" not in df.columns or "gas" not in df.columns:
            return None, None, None, None
        hourly = pd.DataFrame(index=df.index)
        hourly["burner_load_hourly"] = df["burner_load"].astype(float)
        gas_hourly = df["gas"]
        k_fit, metrics = calibrate_k(hourly["burner_load_hourly"], gas_hourly)
        if metrics.get("n_points", 0) < 2:
            return None, None, None, None
        return float(k_fit), metrics, hourly, gas_hourly

    combined, _, _ = fetch_all_series(
        config,
        start,
        end,
        include_gas=False,
        raw_interval=raw_interval,
        include_burner_temps_for_cleaning=False,
        include_fan2_for_operational=only_100pct_operational,
    )
    if combined is None or combined.empty:
        return None, None, None, None

    if not match_reference_pipeline:
        combined, _ = filter_working_time(combined)
    # Fallback path: no 4-level cleaning (pipeline path does it; here we use raw 1m).

    # Hourly: mean(burner_load) over full calendar hour; 100% = 60 operational minutes (s_run & fan1>0 & fan2>0).
    # For 100% hours, mean over full hour = mean over operational minutes (same 60 rows). Matches reference.
    hourly = hourly_for_calibration(
        combined,
        min_s_run_mean=0.01,
        min_operational_minutes=60 if only_100pct_operational else None,
    )
    if hourly.empty:
        return None, None, None, None

    # Gas: same as reference backend — raw points, align by timestamp (hourly['gas'] = gas.reindex(hourly.index)).
    gas_raw = query_energy_gas_raw(config, start, end)
    if gas_raw is None or gas_raw.empty:
        return None, None, None, None
    if gas_raw.index.tz is None and hourly.index.tz is not None:
        gas_raw = gas_raw.tz_localize("UTC")
    elif gas_raw.index.tz is not None and hourly.index.tz is None:
        hourly.index = hourly.index.tz_localize("UTC")
    gas_hourly = gas_raw.reindex(hourly.index)
    k_fit, metrics = calibrate_k(hourly["burner_load_hourly"], gas_hourly)
    if metrics["n_points"] < 2:
        return None, None, None, None

    return float(k_fit), metrics, hourly, gas_hourly
