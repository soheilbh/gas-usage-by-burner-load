"""
Pipeline: fetch s_run, fan1, fan2, burner module → 4-level burner cleaning → 100% operational hours.
Same logic as reference (operational = s_run & fan1>0 & fan2>0; 60 min/hour). Used for query and calibration.
All Influx queries live in influx_queries.py; this module only does fetch orchestration, cleaning, aggregation.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from .config import InfluxConfig
from .influx_queries import fetch_pipeline_1m, query_energy_gas_raw

logger = logging.getLogger(__name__)

# Burner module only (4-level cleaning). Operational filter uses s_run + fan1/fan2.
BURNER_MODULE = {
    "sensors": ["burner_temp1", "burner_temp2", "burner_temp_sp", "burner_div_press", "burner_load"],
    "max_rate": {"burner_temp1": 5, "burner_temp2": 5},
    "cross_validate": [("burner_temp1", "burner_temp2", 0.9)],
}

PIPELINE_SENSORS = (
    ["s_run", "fan1_speed_hz", "fan2_speed_hz"]
    + BURNER_MODULE["sensors"]
)


def _apply_physical_constraints(df: pd.DataFrame) -> pd.DataFrame:
    limits = {
        "temp": (0, 1200),
        "burner_temp": (0, 200),
        "drying_temp": (0, 150),
        "fan_air_temp": (0, 150),
        "product_temp": (0, 100),
        "ambiant_temp": (-20, 50),
        "mcc_temp": (0, 100),
        "div_press": (-10, 10),
        "speed": (0, 60),
        "belt_speed": (0, 10),
        "humidity": (0, 100),
        "moisture": (0, 100),
        "input%": (0, 100),
        "load": (0, 100),
        "capacity": (0, 10000),
        "current": (0, 100),
        "filling_speed": (0, 100),
        "througput": (0, 10000),
    }
    for col in df.columns:
        col_lower = col.lower()
        limit = None
        for key, (lo, hi) in limits.items():
            if key in col_lower:
                limit = (lo, hi)
                break
        if limit is not None:
            df[col] = df[col].clip(lower=limit[0], upper=limit[1])
    return df


def _apply_rate_of_change(df: pd.DataFrame, max_rates: dict) -> pd.DataFrame:
    for sensor, max_rate in max_rates.items():
        if sensor not in df.columns:
            continue
        rate = df[sensor].diff().abs()
        violations = rate > max_rate
        if violations.any():
            df.loc[violations, sensor] = np.nan
            df[sensor] = df[sensor].interpolate(method="linear", limit=3, limit_direction="both")
    return df


def _apply_percentile_capping(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype not in ("float64", "float32", "int64", "int32") or df[col].isna().all():
            continue
        pct = 99.9 if ("fan" in col.lower() and "temp" in col.lower()) else 99.5
        lo = df[col].quantile((100 - pct) / 100)
        hi = df[col].quantile(pct / 100)
        df[col] = df[col].clip(lower=lo, upper=hi)
    return df


def _clean_module(df_all: pd.DataFrame, module_name: str, module_config: dict) -> Optional[pd.DataFrame]:
    sensors = [s for s in module_config["sensors"] if s in df_all.columns]
    if not sensors:
        return None
    df = df_all[sensors].copy()
    df = _apply_physical_constraints(df)
    df = _apply_rate_of_change(df, module_config.get("max_rate", {}))
    # cross_validate is diagnostic only (no value change in reference script)
    df = _apply_percentile_capping(df)
    return df


def run_pipeline(
    config: InfluxConfig,
    start: datetime,
    end: datetime,
    only_100pct: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Fetch s_run, fan1, fan2, burner module → 4-level burner cleaning → operational aggregate.
    only_100pct: if True (default), return only hours with 60 operational minutes; if False, return all hours.
    Returns hourly DataFrame (burner_load, gas, ...; and operational_minutes when only_100pct=False).
    """
    df_all = fetch_pipeline_1m(config, start, end, PIPELINE_SENSORS)
    if df_all is None or df_all.empty:
        return None

    burner_cleaned = _clean_module(df_all, "burner", BURNER_MODULE)
    if burner_cleaned is None:
        return None

    df_merged = burner_cleaned.copy()
    for col in ("s_run", "fan1_speed_hz", "fan2_speed_hz"):
        if col in df_all.columns:
            df_merged[col] = df_all[col].reindex(df_merged.index).ffill().bfill()
    if "s_run" not in df_merged.columns:
        fan1_ok = (df_merged["fan1_speed_hz"] > 0).fillna(False) if "fan1_speed_hz" in df_merged.columns else False
        fan2_ok = (df_merged["fan2_speed_hz"] > 0).fillna(False) if "fan2_speed_hz" in df_merged.columns else False
        df_merged["s_run"] = (fan1_ok & fan2_ok).astype(float)

    s_run_prev = df_merged["s_run"].shift(1).fillna(0)
    df_merged["is_startup_minute"] = ((s_run_prev == 0) & (df_merged["s_run"] > 0)).astype(int)
    s_run_ok = (df_merged["s_run"] > 0).fillna(False)
    fan1_ok = (df_merged["fan1_speed_hz"] > 0).fillna(False) if "fan1_speed_hz" in df_merged.columns else True
    fan2_ok = (df_merged["fan2_speed_hz"] > 0).fillna(False) if "fan2_speed_hz" in df_merged.columns else True
    df_merged["is_operational"] = s_run_ok & fan1_ok & fan2_ok

    exclude = {"s_run", "is_startup_minute", "is_operational"}

    def operational_mean(series: pd.Series) -> float:
        op_mask = df_merged.loc[series.index, "is_operational"]
        vals = series[op_mask]
        return vals.mean() if len(vals) > 0 else np.nan

    agg_dict = {}
    for c in df_merged.columns:
        if c in exclude:
            continue
        agg_dict[c] = operational_mean
    agg_dict["s_run"] = "mean"
    agg_dict["is_operational"] = "sum"
    agg_dict["is_startup_minute"] = "max"
    hourly = df_merged.resample("1h", label="right", closed="right").agg(agg_dict)
    hourly = hourly.rename(columns={"s_run": "s_run_uptime_pct", "is_operational": "operational_minutes", "is_startup_minute": "is_startup_hour"})

    gas = query_energy_gas_raw(config, start, end)
    if gas is not None:
        # Align timezone: gas from Influx is often UTC; hourly index may be naive → reindex fails.
        if gas.index.tz is not None and hourly.index.tz is None:
            gas = gas.tz_localize(None)
        elif gas.index.tz is None and hourly.index.tz is not None:
            gas = gas.tz_localize("UTC").tz_convert(hourly.index.tz)
        hourly["gas"] = gas.reindex(hourly.index)

    if only_100pct:
        out = hourly[hourly["operational_minutes"] == 60].copy()
        out = out.drop(columns=["s_run_uptime_pct", "operational_minutes"], errors="ignore")
        return out
    return hourly
