# Gas usage app — full report (0–100)

End-to-end description of the **Gas usage from burner load (Farmsum)** app: data, pipeline, model, calibration, UI, and validation.

---

## 1. Purpose and scope

- **Goal:** Estimate natural gas consumption (m³) from BD361-0 **burner_load** for a given time range.
- **Model:** `gas_est = K × burner_load × (op_min/60)` where **op_min** = operational minutes in that hour (s_run + fan1 + fan2).
- **Data:** All data from **InfluxDB** (no reference CSV). K can be **calibrated** from measured gas in InfluxDB for a chosen period.
- **Outputs:** Total estimated gas, cost, charts, and tables (hourly / weekly / monthly) with CSV download.

---

## 2. Data sources (InfluxDB)

- **BD361-0** (1m): `s_run`, `fan1_speed_hz`, `fan2_speed_hz`, burner module (burner_load, burner_temp1/2, burner_temp_sp, burner_div_press).
- **energy_data:** `type='gas'` (raw points), aligned to hourly for comparison and calibration.
- Connection: Host, port, database, RP, SSL in app **Connection** expander or env (`INFLUXDB_HOST`, `INFLUXDB_PORT`, `INFLUXDB_DATABASE`, etc.).

---

## 3. Pipeline overview

| Step | What happens |
|------|----------------|
| 1. Fetch | 1m data (s_run, fan1, fan2, burner module) + gas (raw). |
| 2. Clean | 4-level cleaning on **burner module** only (physical, rate, cross-check, percentile). |
| 3. Operational | Per minute: operational = (s_run > 0) and (fan1 > 0) and (fan2 > 0). |
| 4. Aggregate | Resample to 1h: burner_load = mean over **operational minutes**; **operational_minutes** = sum of operational minutes in that hour (0–60). |
| 5. Gas | Attach gas from Influx (reindex to hourly; timezone aligned). |
| 6. Output | Return **all hours** (no filter) so the app can scale gas by (op_min/60). |

Pipeline entry: **full_cleaning_pipeline.run_pipeline(config, start, end, only_100pct=False)**. For **calibration** we use `only_100pct=True` (100% operational hours only).

---

## 4. Fetch (what we get)

- **influx_queries.py:** `fetch_pipeline_1m()` for s_run, fan1, fan2, burner module at 1m; `query_energy_gas_raw()` for gas (no GROUP BY).
- Burner series: 1m, `FILL(previous)`.
- Gas is reindexed to the pipeline’s hourly index (timezone-normalised so UTC gas matches naive hourly if needed).

---

## 5. Cleaning (burner module only)

We do **not** drop 1m rows. We correct values with **4-level cleaning** on the burner module:

| Level | What we do |
|-------|------------|
| 1. Physical | Clip to equipment limits (burner_load 0–100, temps 0–200, div_press −10–10). |
| 2. Rate-of-change | If \|Δ\| > max_rate (°C/min for temps), set to NaN then interpolate (limit=3). |
| 3. Cross-validate | burner_temp1 vs burner_temp2 (diagnostic; no value change). |
| 4. Percentile | Cap at 0.5–99.5% (99.9% for fan temps). |

s_run and fan speeds are **not** cleaned; they are used as-is for the operational filter.

---

## 6. Operational definition

- **Operational minute** = (s_run > 0) and (fan1_speed_hz > 0) and (fan2_speed_hz > 0).
- **operational_minutes** (per hour) = number of such minutes in that hour (0–60).
- **100% operational hour** = hour with **60 operational minutes** (used only for **calibration**).

---

## 7. Hourly aggregation and gas estimation

- **Hourly:** Resample 1h, label=right, closed=right. For burner_load we take the **mean over operational minutes only** in that hour.
- **operational_minutes** is stored per hour (0–60).
- **Gas estimation (Run):**  
  `gas_usage_est_hourly = K × burner_load_hourly × (op_min/60)`  
  So partial hours (e.g. 30 min) contribute proportionally; full hours use full K×load.
- **Cost:** `cost_hourly = gas_usage_est_hourly × gas_price` (€/m³ from Settings).

---

## 8. Calibration (finding K)

- **Input:** Period (From / To) in the **Calibrate K** dialog.
- **Data:** Same pipeline with **only_100pct=True** (100% operational hours) so gas and burner_load are comparable.
- **Model:** `gas ≈ K × burner_load` (no intercept). K fitted by least squares.
- **Output:** New K and metrics (MAE, R², MAPE, n). Calibration expander shows estimated vs measured and the info box shows K and totals.
- **No reference CSV** — calibration uses only InfluxDB for the chosen period.

---

## 9. UI (Streamlit)

- **Title:** Gas usage from burner load (Farmsum), **V{version}** (from app_settings).
- **Connection:** InfluxDB host, port, database, RP, SSL (expander).
- **Time range:** Start/End date and time.
- **Settings:** K and €/m³ (gas price).
- **Actions:** **Run** (query and show results), **Calibrate k** (dialog: From/To dates, Run calibration, Cancel), **Reset**.
- **Results (after Run):**  
  Metrics: Total gas (est.), Total cost, Hours (rows), Operational (min).  
  Chart: Estimated gas (m³/h).  
  **Data table** tabs: **Hourly** | **Weekly** | **Monthly** (with op_min, burner_load_hourly, gas_usage_est_hourly, cost_hourly; weekly/monthly sum gas/cost, sum op_min).
- **Calibration:** Info box (K, MAE, R², MAPE, n). Expander “Estimation vs measured” (chart + table). Calibration expander closes when you click Run.

---

## 10. Outputs and exports

- **Metrics:** Total gas (est.) m³, Total cost €, Hours (rows), Operational (min and equivalent hours).
- **Chart:** Estimated gas (m³/h) time series.
- **Tables:** Hourly (all columns including **op_min**), Weekly (sums), Monthly (sums).
- **CSV download:** Per tab (hourly, weekly, monthly). Columns include timestamp/week_end/month_end, **op_min** (hourly; weekly/monthly = sum), burner_load_hourly (hourly; weekly/monthly = mean), gas_usage_est_hourly, cost_hourly.

---

## 11. Validation (estimated vs actual)

Comparison: **total estimated gas** (sum of K×load×(op_min/60)) vs **total measured gas** from Influx over the same period (no extra filter). Run locally only; **do not publish** actual m³ or dates (confidential).

**Script (internal use):** `python -m gas_usage_app.check_est_vs_actual --multi` (and `--all-hours` for a single period). Output stays on your machine.

**Typical outcome:** On 1–7 month periods, percentage error is usually in the **about −1% to +4%** range; longer spans often sit around **+1–3%**. Exact figures depend on site and period and should not be shared in docs.

---

## 12. Scripts (optional / one-off)

- **check_est_vs_actual.py** — Compare total estimated vs total actual for one period or `--multi` (several periods and % error). Uses same pipeline and op_min scaling. Delete after use if desired.
- **influx_gas_check.py** — Query Influx for raw gas in a period; show point count and timestamps (diagnostic). Delete after use if desired.

---

## 13. File layout

| File | Role |
|------|------|
| **app.py** | Streamlit UI, Run/Calibrate, tabs, metrics, charts, tables. |
| **full_cleaning_pipeline.py** | Fetch, 4-level burner cleaning, operational aggregate, hourly + gas; `only_100pct` for calibration. |
| **find_k_pipeline.py** | Calibration: run pipeline (100% hours), fit K, return metrics and compare df. |
| **processing.py** | `apply_gas_model(hourly, k, gas_price, op_min_col="op_min")`; `calibrate_k()`. |
| **influx_queries.py** | All Influx queries (1m fetch, gas raw, etc.). |
| **config.py** | InfluxConfig. |
| **app_settings.py** | APP_VERSION, DEFAULT_K, DEFAULT_GAS_PRICE_EUR_PER_M3, CALIBRATION_START_DATE, CALIBRATION_END_DATE. |
| **run.py** | Entry: `streamlit run gas_usage_app/app.py --server.port=8502`. |

---

## 14. Summary table

| Term | Meaning |
|------|--------|
| **Cleaning** | 4-level correction on burner module (physical, rate, percentile); no 1m row dropping. |
| **Operational** | s_run > 0 and fan1 > 0 and fan2 > 0 per minute. |
| **op_min** | Operational minutes in that hour (0–60). |
| **Gas estimation** | K × burner_load × (op_min/60); partial hours scaled. |
| **100% hour** | 60 operational minutes; used only for **calibration**. |
| **Run** | All hours, op_min and gas_est per hour; totals and tables. |
| **Calibrate** | 100% hours only; fit K from (burner_load, gas); store K and metrics. |
| **Pipeline** | Fetch → 4-level burner cleaning → operational aggregate → hourly + gas → return all hours (or 100% for calibration). |

---

*Report covers the app from data sources to UI and validation. For run instructions and config, see **README.md**.*
