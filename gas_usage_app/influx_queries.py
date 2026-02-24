"""
InfluxDB query building and execution.
All query construction is centralized here. Uses HTTP query API so we can
point to local or remote InfluxDB (host, port, db name configurable).
"""
import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import requests

from .config import InfluxConfig

logger = logging.getLogger(__name__)

# Default query interval for downsampling (e.g. 1m for raw then we resample to 1H in app)
DEFAULT_INTERVAL = "1m"
QUERY_TIMEOUT = 600


def _format_time(dt: datetime) -> str:
    """Format datetime for InfluxQL (RFC3339)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_query(config: InfluxConfig, query: str) -> Optional[pd.Series]:
    """Execute InfluxQL query and return a single series as pandas Series (index=time)."""
    url = f"{config.base_url()}/query"
    params = {"db": config.database, "q": query}
    if config.username and config.password:
        params["u"] = config.username
        params["p"] = config.password
    try:
        resp = requests.get(url, params=params, timeout=QUERY_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("InfluxDB query failed: status %s", resp.status_code)
            return None
        data = resp.json()
        if not data.get("results"):
            return None
        result = data["results"][0]
        if "series" not in result or not result["series"]:
            return None
        series = result["series"][0]
        df = pd.DataFrame(series["values"], columns=series["columns"])
        time_col = "time"
        if time_col not in df.columns:
            time_col = next(c for c in df.columns if c != "time" and "value" in c.lower() or "mean" in c.lower())
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time")
        # Use first numeric column as value
        value_col = next((c for c in df.columns if c != "time" and df[c].dtype in ("float64", "int64")), df.columns[0])
        return df[value_col]
    except Exception as e:
        logger.exception("InfluxDB query error: %s", e)
        return None


def query_energy_gas(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    interval: str = "1h",
) -> Optional[pd.Series]:
    """Energy gas (reference/validation): mean(value) from energy_data where type='gas'."""
    t_start = _format_time(start)
    t_end = _format_time(end)
    query = f'''
    SELECT mean("value") AS "mean_value"
    FROM "{config.retention_policy}"."energy_data"
    WHERE time >= '{t_start}' AND time <= '{t_end}' AND "type"='gas'
    GROUP BY time({interval}) FILL(null)
    '''
    return _run_query(config, query)


def query_energy_gas_raw(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
) -> Optional[pd.Series]:
    """Raw gas points (no GROUP BY). Used to match backend CSV: hourly['gas'] = gas (align by timestamp)."""
    t_start = _format_time(start)
    t_end = _format_time(end)
    query = f'''
    SELECT value
    FROM "{config.retention_policy}"."energy_data"
    WHERE time >= '{t_start}' AND time <= '{t_end}' AND "type"='gas'
    '''
    return _run_query(config, query)


def _query_bd361_unit_field(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    unit_name: str,
    interval: str,
    fill: str,
    use_last: bool,
    field: str,
) -> Optional[pd.Series]:
    """Query BD361-0 with a specific field (value_f or value_b)."""
    t_start = _format_time(start)
    t_end = _format_time(end)
    agg = f'LAST("{field}") AS "mean_value_f"' if use_last else f'mean("{field}") AS "mean_value_f"'
    query = f'''
    SELECT {agg}
    FROM "{config.retention_policy}"."BD361-0"
    WHERE time >= '{t_start}' AND time <= '{t_end}' AND "unit"='{unit_name}'
    GROUP BY time({interval}) FILL({fill})
    '''
    ser = _run_query(config, query)
    if ser is not None and field == "value_b":
        # Convert boolean to float 0/1 for consistency
        ser = ser.map(lambda x: 1.0 if x in (True, "true", 1, "1") else 0.0).astype(float)
    return ser


def query_bd361_unit(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    unit_name: str,
    interval: str = DEFAULT_INTERVAL,
    fill: str = "null",
    use_last: bool = False,
) -> Optional[pd.Series]:
    """Query BD361-0. use_last=True matches backend (SELECT LAST(value_f) GROUP BY time(1m) FILL(previous))."""
    return _query_bd361_unit_field(config, start, end, unit_name, interval, fill, use_last, "value_f")


def query_burner_load(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    interval: str = DEFAULT_INTERVAL,
    fill: str = "null",
    use_last: bool = False,
) -> Optional[pd.Series]:
    """Burner load from BD361-0 where unit='burner_load'."""
    return query_bd361_unit(config, start, end, "burner_load", interval, fill, use_last)


def query_s_run(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    interval: str = DEFAULT_INTERVAL,
    fill: str = "null",
    use_last: bool = False,
) -> Optional[pd.Series]:
    """Working-time signal: s_run from BD361-0. Tries value_f then value_b (boolean)."""
    ser = _query_bd361_unit_field(config, start, end, "s_run", interval, fill, use_last, "value_f")
    if ser is not None:
        return ser
    return _query_bd361_unit_field(config, start, end, "s_run", interval, fill, use_last, "value_b")


def query_fan1_speed_hz(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    interval: str = DEFAULT_INTERVAL,
    fill: str = "null",
    use_last: bool = False,
) -> Optional[pd.Series]:
    """Fan speed: fan1_speed_hz from BD361-0."""
    return query_bd361_unit(config, start, end, "fan1_speed_hz", interval, fill, use_last)


def query_fan2_speed_hz(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    interval: str = DEFAULT_INTERVAL,
    fill: str = "null",
    use_last: bool = False,
) -> Optional[pd.Series]:
    """Fan speed: fan2_speed_hz from BD361-0 (operational filter: fan1>0 and fan2>0)."""
    return query_bd361_unit(config, start, end, "fan2_speed_hz", interval, fill, use_last)


def fetch_pipeline_1m(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    sensor_names: list,
) -> Optional[pd.DataFrame]:
    """
    Fetch 1m data for given BD361-0 unit names (e.g. s_run, fan1_speed_hz, burner_load, ...).
    Uses GROUP BY time(1m) FILL(previous), LAST. s_run uses value_b if value_f missing.
    Returns DataFrame with one column per successfully fetched sensor, index=time.
    """
    fill = "previous"
    use_last = True
    interval = "1m"
    series_list = []
    for name in sensor_names:
        if name == "s_run":
            ser = query_s_run(config, start, end, interval, fill=fill, use_last=use_last)
        else:
            ser = query_bd361_unit(config, start, end, name, interval, fill=fill, use_last=use_last)
        if ser is not None:
            series_list.append(ser.rename(name))
    if not series_list:
        return None
    df = pd.concat(series_list, axis=1)
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()
    if "s_run" in df.columns:
        if df["s_run"].dtype == object or str(df["s_run"].dtype) == "bool":
            df["s_run"] = df["s_run"].replace({True: 1.0, False: 0.0, "True": 1.0, "False": 0.0}).astype(float)
    return df


def fetch_all_series(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    include_gas: bool = True,
    raw_interval: str = DEFAULT_INTERVAL,
    include_burner_temps_for_cleaning: bool = False,
    include_fan2_for_operational: bool = False,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], dict]:
    """
    Fetch burner_load, s_run, fan1_speed_hz (and optionally gas, burner temps, fan2).
    If include_burner_temps_for_cleaning=True, also fetches burner_temp1, burner_temp2, fan2_speed_hz
    for 4-level burner cleaning.
    If include_fan2_for_operational=True, fetches fan2_speed_hz (for reference-aligned operational filter).
    Uses FILL(previous) for 1m so we get one value per minute (s_run etc. are event-like: 1 until 0).
    Returns (combined_df, gas_series, stats).
    """
    stats = {"burner_load_points": 0, "s_run_points": 0, "fan1_points": 0, "gas_points": 0}

    # 1m: FILL(previous) so we have one value per minute (s_run stays 1 until it goes 0; sparse in DB)
    fill_1m = "previous" if raw_interval == "1m" else "null"
    use_last_1m = raw_interval == "1m"

    burner = query_burner_load(config, start, end, raw_interval, fill=fill_1m, use_last=use_last_1m)
    s_run = query_s_run(config, start, end, raw_interval, fill=fill_1m, use_last=use_last_1m)
    fan1 = query_fan1_speed_hz(config, start, end, raw_interval, fill=fill_1m, use_last=use_last_1m)
    series_list = [("burner_load", burner), ("s_run", s_run), ("fan1_speed_hz", fan1)]

    if include_burner_temps_for_cleaning:
        bt1 = query_bd361_unit(config, start, end, "burner_temp1", raw_interval, fill=fill_1m, use_last=use_last_1m)
        bt2 = query_bd361_unit(config, start, end, "burner_temp2", raw_interval, fill=fill_1m, use_last=use_last_1m)
        fan2 = query_fan2_speed_hz(config, start, end, raw_interval, fill=fill_1m, use_last=use_last_1m)
        series_list.extend([("burner_temp1", bt1), ("burner_temp2", bt2), ("fan2_speed_hz", fan2)])
    elif include_fan2_for_operational:
        fan2 = query_fan2_speed_hz(config, start, end, raw_interval, fill=fill_1m, use_last=use_last_1m)
        if fan2 is not None:
            series_list.append(("fan2_speed_hz", fan2))

    if burner is not None:
        stats["burner_load_points"] = int(burner.notna().sum())
    if s_run is not None:
        stats["s_run_points"] = int(s_run.notna().sum())
    if fan1 is not None:
        stats["fan1_points"] = int(fan1.notna().sum())

    gas_series = None
    if include_gas:
        gas_series = query_energy_gas(config, start, end, interval="1h")
        if gas_series is not None:
            stats["gas_points"] = int(gas_series.notna().sum())

    dfs = []
    for name, ser in series_list:
        if ser is not None:
            dfs.append(ser.rename(name))
    if not dfs:
        return None, gas_series, stats

    combined = pd.concat(dfs, axis=1)
    combined = combined[~combined.index.duplicated(keep="first")]
    combined = combined.sort_index()

    return combined, gas_series, stats
