"""
Streamlit app: Gas usage from burner load (Farmsum).
Estimate gas usage (m³/h) from burner_load; calibrate K from InfluxDB.
Run from repo root: streamlit run app.py
"""
import logging
import os
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
import streamlit as st

from gas_usage.config import InfluxConfig
from gas_usage.app_settings import (
    APP_VERSION,
    CALIBRATION_END_DATE,
    CALIBRATION_START_DATE,
    DEFAULT_GAS_PRICE_EUR_PER_M3,
    DEFAULT_K,
)
from gas_usage.find_k_pipeline import run_find_k
from gas_usage.full_cleaning_pipeline import run_pipeline
from gas_usage.processing import apply_gas_model
from gas_usage.user_prefs import get_effective_default_k, save_default_k

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_APP_ROOT = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(page_title="Gas usage from burner load (Farmsum)", layout="wide")

if "k" not in st.session_state:
    st.session_state["k"] = get_effective_default_k(_APP_ROOT, DEFAULT_K)
if "result_df" not in st.session_state:
    st.session_state["result_df"] = None
if "hourly_raw" not in st.session_state:
    st.session_state["hourly_raw"] = None
if "gas_measured" not in st.session_state:
    st.session_state["gas_measured"] = None
if "cleaning_stats" not in st.session_state:
    st.session_state["cleaning_stats"] = {}
if "fetch_stats" not in st.session_state:
    st.session_state["fetch_stats"] = {}
if "gas_price" not in st.session_state:
    st.session_state["gas_price"] = DEFAULT_GAS_PRICE_EUR_PER_M3


def get_influx_config() -> InfluxConfig:
    return InfluxConfig(
        host=st.session_state.get("influx_host", os.getenv("INFLUXDB_HOST", "localhost")),
        port=str(st.session_state.get("influx_port", os.getenv("INFLUXDB_PORT", "8087"))),
        database=st.session_state.get("influx_database", os.getenv("INFLUXDB_DATABASE", "farmsum_db")),
        retention_policy=st.session_state.get("influx_rp", os.getenv("INFLUXDB_RETENTION_POLICY", "autogen")),
        username=os.getenv("INFLUXDB_USERNAME") or None,
        password=os.getenv("INFLUXDB_PASSWORD") or None,
        ssl=st.session_state.get("influx_ssl", False),
    )


def run_query():
    config = get_influx_config()
    start = st.session_state.get("query_start")
    end = st.session_state.get("query_end")
    if not start or not end or start >= end:
        st.error("Please select a valid time range (start < end).")
        return
    k = st.session_state["k"]
    gas_price = st.session_state.get("gas_price", 0.0)
    with st.spinner("Querying InfluxDB..."):
        hourly_all = run_pipeline(config, start, end, only_100pct=False)
    if hourly_all is None or hourly_all.empty:
        st.error("No data returned. Check InfluxDB and time range.")
        return
    n_hours = len(hourly_all)
    st.session_state["fetch_stats"] = {"hours_100pct": n_hours}
    st.session_state["cleaning_stats"] = {"pipeline": "pipeline", "hours_100pct": n_hours, "points_after": n_hours}
    gas_measured = hourly_all["gas"] if "gas" in hourly_all.columns else None
    st.session_state["gas_measured"] = gas_measured
    hourly = pd.DataFrame(index=hourly_all.index)
    hourly["burner_load_hourly"] = hourly_all["burner_load"].astype(float)
    # Operational minutes in that hour (s_run + fan1 + fan2); scale gas est by (op_min/60).
    op_min = hourly_all["operational_minutes"] if "operational_minutes" in hourly_all.columns else pd.Series(60.0, index=hourly.index)
    hourly["op_min"] = op_min.reindex(hourly.index).fillna(0).clip(0, 60)
    st.session_state["hourly_raw"] = hourly
    st.session_state["result_df"] = apply_gas_model(hourly, k, gas_price, op_min_col="op_min")
    st.session_state["result_query_start"] = start
    st.session_state["result_query_end"] = end
    st.session_state["calibration_expander_expanded"] = False


def run_calibrate(cal_start, cal_end):
    if not cal_start or not cal_end or cal_start >= cal_end:
        return False, "Set a valid calibration period (From < To)."
    config = get_influx_config()
    with st.spinner("Calibrating..."):
        k_fit, metrics, hourly, gas_hourly = run_find_k(
            config, cal_start, cal_end,
            only_100pct_operational=True,
            use_burner_cleaning=True,
        )
    if k_fit is None:
        return False, "Could not calibrate: no data or not enough overlap."
    st.session_state["k"] = k_fit
    st.session_state["k_input"] = k_fit  # sync widget so it shows new K instead of old value
    st.session_state["calibration_metrics"] = metrics
    compare_df = hourly[["burner_load_hourly"]].copy()
    compare_df["gas_estimated_hourly"] = compare_df["burner_load_hourly"] * k_fit
    compare_df["gas_measured_hourly"] = gas_hourly
    compare_df = compare_df.dropna(how="all").dropna(subset=["gas_measured_hourly"])
    st.session_state["calibration_compare_df"] = compare_df if not compare_df.empty else None
    st.session_state["calibration_expander_expanded"] = True
    if st.session_state.get("hourly_raw") is not None and not st.session_state["hourly_raw"].empty:
        st.session_state["result_df"] = apply_gas_model(
            st.session_state["hourly_raw"], k_fit, st.session_state.get("gas_price", 0.0), op_min_col="op_min"
        )
    st.session_state["calibration_offer_save_default"] = True
    return True, None


def _parse_date(s: str):
    """Parse YYYY-MM-DD to date or return None."""
    if not s or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


@st.dialog("Save as default?", width="small")
def save_default_dialog():
    """Ask user whether to persist the calibrated K as default for new sessions."""
    k = st.session_state.get("k", 0)
    st.markdown(f"Calibration done. **K = {k:.2f}**")
    st.caption("Save this K as the default for new sessions?")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Yes, save as default", type="primary", use_container_width=True):
            if save_default_k(_APP_ROOT, k):
                st.session_state.pop("calibration_offer_save_default", None)
                st.rerun()
            else:
                st.error("Could not save (check write permission for data/).")
    with col2:
        if st.button("No", use_container_width=True):
            st.session_state.pop("calibration_offer_save_default", None)
            st.rerun()


def _init_cal_dialog_dates():
    """Ensure calibration dialog date keys exist; use app defaults as initial value."""
    if "cal_dialog_start" not in st.session_state:
        st.session_state["cal_dialog_start"] = CALIBRATION_START_DATE.isoformat()
    if "cal_dialog_end" not in st.session_state:
        st.session_state["cal_dialog_end"] = CALIBRATION_END_DATE.isoformat()


@st.dialog("Calibrate K", width="medium")
def calibrate_dialog():
    _init_cal_dialog_dates()
    st.caption("Pick a period with measured gas in InfluxDB. K will be fitted from burner_load vs gas.")
    st.text_input("From (YYYY-MM-DD)", key="cal_dialog_start")
    st.text_input("To (YYYY-MM-DD)", key="cal_dialog_end")
    # Read from session state (source of truth; text_input updates it)
    from_str = st.session_state.get("cal_dialog_start", "")
    to_str = st.session_state.get("cal_dialog_end", "")
    cal_start_date = _parse_date(from_str)
    cal_end_date = _parse_date(to_str)
    if cal_start_date is None:
        st.warning("From: use YYYY-MM-DD (e.g. 2024-02-01).")
    if cal_end_date is None:
        st.warning("To: use YYYY-MM-DD (e.g. 2024-09-15).")
    cal_start = datetime.combine(cal_start_date, datetime.min.time()) if cal_start_date else None
    cal_end = datetime.combine(cal_end_date, datetime.max.time()) if cal_end_date else None
    if cal_start and cal_end:
        st.caption(f"→ Calibration period: **{cal_start.date()}** to **{cal_end.date()}**")
    col1, col2 = st.columns([2, 1])
    with col1:
        if st.button("Run calibration", type="primary", use_container_width=True):
            # Use session state directly so user-edited dates are definitely passed
            fs = st.session_state.get("cal_dialog_start", "")
            ts = st.session_state.get("cal_dialog_end", "")
            cs = _parse_date(fs)
            ce = _parse_date(ts)
            start = datetime.combine(cs, datetime.min.time()) if cs else None
            end = datetime.combine(ce, datetime.max.time()) if ce else None
            ok, msg = run_calibrate(start, end)
            if ok:
                st.rerun()
            else:
                st.warning(msg)
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.rerun()


tit_col, ver_col = st.columns([1, 0.06])
with tit_col:
    st.title("Gas usage from burner load (Farmsum)")
with ver_col:
    st.markdown(f'<p style="text-align: right; margin-top: 0.5rem;"><strong>V{APP_VERSION}</strong></p>', unsafe_allow_html=True)

with st.expander("Connection (InfluxDB)", expanded=False):
    c1, c2, c3, c4, c5 = st.columns([2, 1, 2, 1, 1])
    with c1:
        st.text_input("Host", key="influx_host", value=os.getenv("INFLUXDB_HOST", "localhost"))
    with c2:
        st.text_input("Port", key="influx_port", value=os.getenv("INFLUXDB_PORT", "8087"))
    with c3:
        st.text_input("Database", key="influx_database", value=os.getenv("INFLUXDB_DATABASE", "farmsum_db"))
    with c4:
        st.text_input("RP", key="influx_rp", value=os.getenv("INFLUXDB_RETENTION_POLICY", "autogen"))
    with c5:
        st.checkbox("SSL", key="influx_ssl", value=False)

st.markdown("---")
tool1, tool2, tool4 = st.columns([2, 1.6, 1.2])
with tool1:
    st.markdown("**Time range**")
    d1, d2 = st.columns(2)
    with d1:
        date_start = st.date_input("Start date", value=datetime.now().date() - timedelta(days=7), key="date_start")
        time_start = st.time_input("Start time", value=datetime.min.time(), key="time_start")
    with d2:
        date_end = st.date_input("End date", value=datetime.now().date(), key="date_end")
        time_end = st.time_input("End time", value=datetime.max.time(), key="time_end")
    st.session_state["query_start"] = datetime.combine(date_start, time_start)
    st.session_state["query_end"] = datetime.combine(date_end, time_end)
with tool2:
    st.markdown("**Settings**")
    k_col, price_col = st.columns(2)
    with k_col:
        k = st.number_input("K", min_value=0.0, value=st.session_state["k"], step=0.01, key="k_input", format="%.2f")
        st.session_state["k"] = k
    with price_col:
        st.number_input("€/m³", min_value=0.0, step=0.01, key="gas_price", format="%.2f")
with tool4:
    st.markdown("**Actions**")
    if st.button("Run", type="primary", use_container_width=True):
        run_query()
    if st.session_state.get("calibration_offer_save_default"):
        save_default_dialog()
    elif st.button("Calibrate k", use_container_width=True):
        calibrate_dialog()
    if st.button("Reset", use_container_width=True):
        st.session_state["result_df"] = None
        st.session_state["hourly_raw"] = None
        st.session_state["gas_measured"] = None
        st.session_state["result_query_start"] = None
        st.session_state["result_query_end"] = None
        st.session_state["calibration_compare_df"] = None
        st.session_state["cleaning_stats"] = {}
        st.session_state["fetch_stats"] = {}
        st.rerun()

st.markdown("---")

hourly_raw = st.session_state.get("hourly_raw")
if hourly_raw is not None and not hourly_raw.empty:
    k_current = st.session_state.get("k", DEFAULT_K)
    gas_price_current = st.session_state.get("gas_price", DEFAULT_GAS_PRICE_EUR_PER_M3)
    st.session_state["result_df"] = apply_gas_model(hourly_raw, k_current, gas_price_current, op_min_col="op_min")

result_df = st.session_state.get("result_df")
result_start = st.session_state.get("result_query_start")
result_end = st.session_state.get("result_query_end")
if result_df is not None and not result_df.empty and result_start and result_end:
    try:
        if result_df.index.tz is not None:
            tz = result_df.index.tz
            start_s = pd.Timestamp(result_start).tz_localize(tz) if getattr(result_start, "tzinfo", None) is None else pd.Timestamp(result_start).tz_convert(tz)
            end_s = pd.Timestamp(result_end).tz_localize(tz) if getattr(result_end, "tzinfo", None) is None else pd.Timestamp(result_end).tz_convert(tz)
            result_df = result_df.loc[start_s:end_s]
        else:
            result_df = result_df.loc[result_start:result_end]
    except Exception:
        pass
if "calibration_metrics" in st.session_state:
    m = st.session_state["calibration_metrics"]
    k_val = st.session_state["k"]
    mae = m.get("mae")
    r2 = m.get("r2")
    mape = m.get("mape_pct")
    n_pts = m.get("n_points")
    mae_s = f"{mae:.2f} m³/h" if mae is not None else "—"
    r2_s = f"{r2:.3f}" if r2 is not None else "—"
    mape_s = f"{mape:.1f}%" if mape is not None else "—"
    n_s = str(int(n_pts)) if n_pts is not None else "—"
    st.info(
        f"**Calibration** · Model: gas = K × burner load  ·  **K** = {k_val:.4f}\n\n"
        f"**MAE** {mae_s}  ·  **R²** {r2_s}  ·  **MAPE** {mape_s}  ·  **n** = {n_s} hours"
    )

cal_compare = st.session_state.get("calibration_compare_df")
if cal_compare is not None and not cal_compare.empty:
    with st.expander("Estimation vs measured (calibration)", expanded=st.session_state.get("calibration_expander_expanded", True)):
        st.line_chart(cal_compare[["gas_estimated_hourly", "gas_measured_hourly"]].rename(columns={"gas_estimated_hourly": "estimated", "gas_measured_hourly": "measured"}), height=280)
        table_df = cal_compare.reset_index().rename(columns={"time": "timestamp"})
        st.dataframe(table_df, use_container_width=True, height=250)

if result_df is not None and not result_df.empty:
    total_gas = result_df["gas_usage_est_hourly"].sum()
    total_cost = result_df["cost_hourly"].sum()
    n_rows = len(result_df)
    op_min_total = int(result_df["op_min"].sum()) if "op_min" in result_df.columns else n_rows * 60
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total gas (est.)", f"{total_gas:,.1f} m³")
    col2.metric("Total cost", f"€ {total_cost:,.2f}")
    col3.metric("Hours (rows)", n_rows)
    col4.metric("Operational (min)", f"{op_min_total:,} ({op_min_total/60:.1f}h)")
    st.subheader("Estimated gas (m³/h)")
    st.line_chart(result_df[["gas_usage_est_hourly"]], height=300)
    st.subheader("Data table")
    tab_h, tab_w, tab_m = st.tabs(["Hourly", "Weekly", "Monthly"])
    with tab_h:
        display_df = result_df.reset_index().rename(columns={"time": "timestamp"})
        st.dataframe(display_df, use_container_width=True, height=300)
        st.download_button("Download hourly CSV", data=display_df.to_csv(index=False), file_name="gas_usage_est_hourly.csv", mime="text/csv", key="dl_hourly")
    with tab_w:
        agg_w = {"burner_load_hourly": "mean", "gas_usage_est_hourly": "sum", "cost_hourly": "sum"}
        if "op_min" in result_df.columns:
            agg_w["op_min"] = "sum"
        weekly = result_df.resample("W", label="right", closed="right").agg(agg_w).dropna(how="all")
        weekly_df = weekly.reset_index().rename(columns={"time": "week_end"})
        weekly_df["gas_usage_est_hourly"] = weekly_df["gas_usage_est_hourly"].round(2)
        st.dataframe(weekly_df, use_container_width=True, height=300)
        st.download_button("Download weekly CSV", data=weekly_df.to_csv(index=False), file_name="gas_usage_est_weekly.csv", mime="text/csv", key="dl_weekly")
    with tab_m:
        agg_m = {"burner_load_hourly": "mean", "gas_usage_est_hourly": "sum", "cost_hourly": "sum"}
        if "op_min" in result_df.columns:
            agg_m["op_min"] = "sum"
        monthly = result_df.resample("ME", label="right", closed="right").agg(agg_m).dropna(how="all")
        monthly_df = monthly.reset_index().rename(columns={"time": "month_end"})
        monthly_df["gas_usage_est_hourly"] = monthly_df["gas_usage_est_hourly"].round(2)
        st.dataframe(monthly_df, use_container_width=True, height=300)
        st.download_button("Download monthly CSV", data=monthly_df.to_csv(index=False), file_name="gas_usage_est_monthly.csv", mime="text/csv", key="dl_monthly")
else:
    st.info("Select a time range and click **Run** to load data.")
