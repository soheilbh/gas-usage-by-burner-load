"""
Persist user preferences (e.g. calibrated K as default) to a JSON file.
Uses data/user_config.json; falls back to app_settings.DEFAULT_K if file missing.
"""
import json
import logging
import os

logger = logging.getLogger(__name__)

# Path relative to app root (where app.py lives)
_CONFIG_DIR = "data"
_CONFIG_FILE = "user_config.json"


def _config_path(app_root: str) -> str:
    return os.path.join(app_root, _CONFIG_DIR, _CONFIG_FILE)


def _load_config(app_root: str) -> dict:
    """Load user config JSON; return {} if missing or invalid."""
    path = _config_path(app_root)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not load user config from %s: %s", path, e)
        return {}


def _save_config(app_root: str, data: dict) -> bool:
    """Save user config; merge with existing to preserve other keys. Returns True on success."""
    path = _config_path(app_root)
    dir_path = os.path.dirname(path)
    existing = _load_config(app_root)
    existing.update(data)
    try:
        os.makedirs(dir_path, exist_ok=True)
        with open(path, "w") as f:
            json.dump(existing, f, indent=2)
        return True
    except OSError as e:
        logger.warning("Could not save user config to %s: %s", path, e)
        return False


def get_effective_default_k(app_root: str, fallback: float) -> float:
    """Load default K from user_config.json if present and valid; else return fallback."""
    data = _load_config(app_root)
    k = data.get("default_k")
    if k is not None and isinstance(k, (int, float)) and k > 0:
        return float(k)
    return fallback


def get_effective_default_gas_price(app_root: str, fallback: float) -> float:
    """Load default gas price from user_config.json if present and valid; else return fallback."""
    data = _load_config(app_root)
    p = data.get("default_gas_price")
    if p is not None and isinstance(p, (int, float)) and p >= 0:
        return float(p)
    return fallback


def save_default_k(app_root: str, k: float) -> bool:
    """Save K as the new default. Merges with existing config."""
    return _save_config(app_root, {"default_k": k})


def save_default_gas_price(app_root: str, gas_price: float) -> bool:
    """Save gas price as the new default. Merges with existing config."""
    return _save_config(app_root, {"default_gas_price": gas_price})
