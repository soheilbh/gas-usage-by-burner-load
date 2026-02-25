"""
One-off: query Influx directly for gas in the same period and show what we get.
Run from repo root: python scripts/influx_gas_check.py
"""
import os
import sys
from datetime import datetime

if __name__ == "__main__":
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, _root)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
from gas_usage.config import InfluxConfig
from gas_usage.influx_queries import query_energy_gas_raw


def main():
    start = datetime(2024, 2, 5)
    end = datetime(2024, 6, 5, 23, 59, 59)
    config = InfluxConfig.from_env()
    print("Querying InfluxDB for raw gas (same query as pipeline)...")
    print(f"  Period: {start.date()} to {end.date()}\n")
    gas = query_energy_gas_raw(config, start, end)
    if gas is None or gas.empty:
        print("No gas series returned. Check InfluxDB and energy_data type='gas'.")
        return
    gas = gas.sort_index()
    n = len(gas)
    print(f"Total gas points returned: {n}")
    print(f"First timestamp: {gas.index[0]}")
    print(f"Last  timestamp: {gas.index[-1]}")
    # Timestamp alignment: do we have one point per hour at :00:00?
    minutes = gas.index.minute
    seconds = gas.index.second
    at_hour = ((minutes == 0) & (seconds == 0)).sum()
    print(f"Points exactly on hour (:00:00): {at_hour} / {n}")
    # Resample to 1h (same as pipeline) and count hours with data
    gas_hourly = gas.resample("1h", label="right", closed="right").sum()
    hours_with_data = gas_hourly.notna() & (gas_hourly > 0)
    n_hours_with = int(hours_with_data.sum())
    print(f"Hours with gas (after resample 1h sum): {n_hours_with}")
    # Sample of raw timestamps (first 15, last 15)
    print("\nFirst 15 raw gas timestamps:")
    for ts in gas.index[:15]:
        print(f"  {ts}")
    print("  ...")
    print("\nLast 15 raw gas timestamps:")
    for ts in gas.index[-15:]:
        print(f"  {ts}")
    # Reindex: gas has tz (UTC). Pipeline hourly index is from resample - check if tz-naive
    hourly_naive = pd.date_range(start=start, end=end, freq="h")
    gas_reindexed_naive = gas.reindex(hourly_naive)
    missing_naive = gas_reindexed_naive.isna().sum()
    # If we normalize to same tz, do we get matches?
    gas_naive = gas.tz_localize(None) if gas.index.tz is not None else gas
    gas_reindexed_naive2 = gas_naive.reindex(hourly_naive)
    missing_naive2 = gas_reindexed_naive2.isna().sum()
    print(f"\nGas index timezone: {gas.index.tz}")
    print(f"Reindex gas (UTC) to naive hourly: {int(missing_naive)} hours with NaN.")
    print(f"Reindex gas (strip tz) to naive hourly: {int(missing_naive2)} hours with NaN.")
    # Pipeline has 1457 hours (100% op); only 1352 have gas. So 105 pipeline hours have no gas.
    # Those 105 are a subset of the 170 calendar hours that have no gas (strip-tz alignment).
    print(f"\n=> So in the full period there are {int(missing_naive2)} hours with NO gas point in Influx.")
    print("  The pipeline's 105 'missing gas' hours are those operational hours that fall in that gap.")


if __name__ == "__main__":
    main()
