# Gas usage from burner load (Farmsum)

> Estimate natural gas consumption (m³) from BD361-0 burner load. K is calibrated from InfluxDB; gas estimate scales by operational minutes per hour.

---

## Quick start

**Requirements:** Python 3.x, InfluxDB with BD361-0 and `energy_data` (gas).

```bash
# From repo root
pip install -r requirements.txt
streamlit run app.py
```

Or run on port 8502:

```bash
python scripts/run.py
```

---

## Layout

| Path | Purpose |
|------|--------|
| **app.py** | Streamlit app (single entry at root). |
| **gas_usage/** | Config, pipelines, Influx queries, processing. |
| **scripts/** | `run.py` (launch app on 8502), `check_est_vs_actual.py`, `influx_gas_check.py`. |

---

## What it does

- **Model:** `gas_est = K × burner_load × (op_min/60)` — partial hours (e.g. 30 min run) count proportionally.
- **Data:** All from InfluxDB (1m burner_load, s_run, fan1, fan2; gas from `energy_data`). No reference CSV.
- **Calibrate K:** Pick a period in the app → fit K from measured gas (100% operational hours only).
- **Outputs:** Total gas (est.), cost, chart, and tables (Hourly | Weekly | Monthly) with CSV download.

---

## Configuration

| Where | What |
|-------|------|
| **App** | Connection expander: InfluxDB host, port, database, RP, SSL. |
| **Env** | `INFLUXDB_HOST`, `INFLUXDB_PORT`, `INFLUXDB_DATABASE`, `INFLUXDB_RETENTION_POLICY`, `INFLUXDB_SSL`. |
| **gas_usage/app_settings.py** | `APP_VERSION`, `DEFAULT_K`, `DEFAULT_GAS_PRICE_EUR_PER_M3`, `CALIBRATION_START_DATE`, `CALIBRATION_END_DATE`. |

---

## Pipeline (short)

1. **Fetch** 1m data (burner_load, s_run, fan1, fan2) + gas.
2. **Clean** burner module (4-level: physical, rate, percentile).
3. **Operational:** `op_min` = minutes per hour with s_run & fan1 & fan2 > 0 (0–60).
4. **Run:** All hours; gas_est = K × load × (op_min/60). **Calibrate:** 100% hours only → fit K.

---

## CSV export

| Tab | Columns (examples) |
|-----|--------------------|
| Hourly | timestamp, **op_min**, burner_load_hourly, gas_usage_est_hourly, cost_hourly |
| Weekly | week_end, op_min (sum), burner_load_hourly (mean), gas_usage_est_hourly (sum), cost_hourly (sum) |
| Monthly | month_end, op_min (sum), burner_load_hourly (mean), gas_usage_est_hourly (sum), cost_hourly (sum) |

---

## Validation

Estimated vs actual total gas over a period: typically **≈ −1% to +4%** error for 1–7 month periods. Validation is for **internal use**; do not publish actual m³ or dates. See **[GAS_USAGE_APP_REPORT.md](GAS_USAGE_APP_REPORT.md)** for the validation section and full report (pipeline, cleaning, UI, scripts).
