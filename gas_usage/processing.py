"""
Gas usage estimation and calibration helpers.
- filter_working_time, hourly_for_calibration: used by find_k_pipeline fallback path.
- apply_gas_model: K × burner_load + cost (used by app after pipeline).
- calibrate_k: fit K from (burner_load, gas) least squares (used by find_k_pipeline).
"""
import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Resampling: label='right', closed='right' so e.g. 13:00 = period [12:00, 13:00).
RESAMPLE_RULE = "1h"
RESAMPLE_AGG = "mean"
RESAMPLE_LABEL = "right"
RESAMPLE_CLOSED = "right"


def filter_working_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Keep only rows where s_run > 0, fan1_speed_hz > 0, and (if present) fan2_speed_hz > 0.
    Operational = s_run > 0 and fan1>0 and fan2>0 (aligned with reference data).
    """
    if df.empty:
        return df, 0
    n_before = len(df)
    mask = True
    if "s_run" in df.columns:
        mask = mask & (df["s_run"] > 0)
    if "fan1_speed_hz" in df.columns:
        mask = mask & (df["fan1_speed_hz"] > 0)
    if "fan2_speed_hz" in df.columns:
        mask = mask & (df["fan2_speed_hz"] > 0)
    df = df.loc[mask]
    removed = n_before - len(df)
    return df, removed


def hourly_for_calibration(
    df: pd.DataFrame,
    min_s_run_mean: float = 0.01,
    min_operational_minutes: Optional[int] = None,
) -> pd.DataFrame:
    """
    Hourly series for calibrating K: mean(burner_load) over the FULL calendar hour,
    then keep hours by activity.

    - min_s_run_mean: keep hours where mean(s_run) >= this (default 0.01).
    - min_operational_minutes: if 59 or 60, keep only hours with that many operational
      minutes. Matches reference create_final_hourly_datasets.py: operational minute =
      s_run>0 and fan1_speed_hz>0 and fan2_speed_hz>0 (if present); 100% = 60 such
      minutes per hour (count-based, threshold > 0 for fans).

    Gas from InfluxDB is per calendar hour; full-hour mean load makes them comparable.
    """
    if df.empty or "burner_load" not in df.columns:
        return pd.DataFrame()
    resample_kw = {"rule": RESAMPLE_RULE, "label": RESAMPLE_LABEL, "closed": RESAMPLE_CLOSED}
    hourly = df[["burner_load"]].resample(**resample_kw).agg(RESAMPLE_AGG)
    hourly = hourly.rename(columns={"burner_load": "burner_load_hourly"})
    if min_operational_minutes is not None and min_operational_minutes >= 59 and "s_run" in df.columns:
        # Match reference: operational = s_run & fan1>0 & fan2>0 per minute; 100% = 60 such minutes.
        # Data is fetched with FILL(previous) so we have one value per minute (s_run is 1 until 0).
        s_run_ok = (df["s_run"] > 0).fillna(False)
        fan1_ok = (df["fan1_speed_hz"] > 0).fillna(False) if "fan1_speed_hz" in df.columns else True
        fan2_ok = (df["fan2_speed_hz"] > 0).fillna(False) if "fan2_speed_hz" in df.columns else True
        is_op = s_run_ok & fan1_ok & fan2_ok
        op_count = is_op.astype(int).resample(**resample_kw).sum()
        mask = (op_count == min_operational_minutes).reindex(hourly.index).fillna(False)
        hourly = hourly.loc[mask]
    elif min_s_run_mean > 0 and "s_run" in df.columns:
        s_run_h = df[["s_run"]].resample(**resample_kw).agg(RESAMPLE_AGG)
        mask = (s_run_h["s_run"] >= min_s_run_mean).reindex(hourly.index).fillna(False)
        hourly = hourly.loc[mask]
    return hourly.dropna(subset=["burner_load_hourly"])


def apply_gas_model(
    hourly: pd.DataFrame,
    k: float,
    gas_price: float,
    intercept: float = 0.0,
    op_min_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    gas_usage_est = (k * burner_load_hourly + intercept) * (op_min/60) when op_min_col given;
    otherwise full-hour equivalent. cost = gas_usage_est * gas_price.
    op_min_col: column name for operational minutes in that hour (s_run+fan1+fan2); scales est by (op_min/60).
    """
    out = hourly.copy()
    base = out["burner_load_hourly"] * k + intercept
    if op_min_col and op_min_col in out.columns:
        op_min = out[op_min_col].fillna(0).clip(0, 60)
        out["gas_usage_est_hourly"] = base * (op_min / 60.0)
    else:
        out["gas_usage_est_hourly"] = base
    out["cost_hourly"] = out["gas_usage_est_hourly"] * gas_price
    return out


def calibrate_k(
    hourly_burner: pd.Series,
    hourly_gas: pd.Series,
) -> Tuple[float, dict]:
    """
    Fit k from gas ≈ k * burner_load using working-time samples only (both must be non-NaN).
    Simple least squares: k = (gas * burner).sum() / (burner^2).sum()
    Returns (fitted_k, metrics dict with MAE, RMSE).
    """
    aligned = pd.concat([hourly_burner.rename("burner"), hourly_gas.rename("gas")], axis=1).dropna()
    if len(aligned) < 2:
        return 0.0, {"mae": None, "rmse": None, "mape_pct": None, "r2": None, "n_points": len(aligned)}
    b = aligned["burner"].values
    g = aligned["gas"].values
    k = np.dot(g, b) / (np.dot(b, b) + 1e-12)
    pred = k * b
    mae = np.abs(g - pred).mean()
    rmse = np.sqrt(np.mean((g - pred) ** 2))
    # MAPE (%) where measured != 0
    mask = np.abs(g) >= 1e-9
    denom = np.abs(g).clip(min=1e-9)
    mape_pct = (np.abs(g - pred) / denom)[mask].mean() * 100.0 if mask.any() else None
    # R² (1 = perfect fit; 0 = no better than mean)
    ss_res = np.sum((g - pred) ** 2)
    ss_tot = np.sum((g - g.mean()) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else None
    return float(k), {
        "mae": float(mae),
        "rmse": float(rmse),
        "mape_pct": float(mape_pct) if mape_pct is not None else None,
        "r2": r2,
        "n_points": len(aligned),
    }
