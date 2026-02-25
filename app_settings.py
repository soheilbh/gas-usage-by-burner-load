"""
Default settings for the Gas usage app.
Edit this file to change default K, gas price, and calibration period
without changing the app code. Restart the app (or refresh) to load new values.
"""
from datetime import date

# ---------------------------------------------------------------------------
# App version (shown under the title).
# ---------------------------------------------------------------------------
APP_VERSION = "1.0"

# ---------------------------------------------------------------------------
# Default calibration factor: gas_usage_est = K * burner_load_hourly (no intercept).
# Used as the starting value when you open the app. In the app you calibrate by
# selecting your own period and clicking Calibrate k (data from InfluxDB; no reference).
# This default (7.97) was computed from InfluxDB for 2024-01-08 to 2024-10-07.
# ---------------------------------------------------------------------------
DEFAULT_K = 7.97

# ---------------------------------------------------------------------------
# Default gas price: Netherlands industrial natural gas (EUR per m³).
# Household was ~0.57 EUR/m³ (Dec 2024); industrial typically lower. Update for your contract.
# ---------------------------------------------------------------------------
DEFAULT_GAS_PRICE_EUR_PER_M3 = 0.50

# ---------------------------------------------------------------------------
# Calibration period: from which date to which date to compute k when you click "Calibrate k".
# ---------------------------------------------------------------------------
CALIBRATION_START_DATE = date(2024, 2, 1)   # 2024-02-01
CALIBRATION_END_DATE = date(2024, 9, 15)    # 2024-09-15
