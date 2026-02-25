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


def get_effective_default_k(app_root: str, fallback: float) -> float:
    """
    Load default K from user_config.json if present and valid; else return fallback.
    app_root: directory containing app.py (repo root).
    """
    path = _config_path(app_root)
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r") as f:
            data = json.load(f)
        k = data.get("default_k")
        if k is not None and isinstance(k, (int, float)) and k > 0:
            return float(k)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not load user config from %s: %s", path, e)
    return fallback


def save_default_k(app_root: str, k: float) -> bool:
    """
    Save K as the new default in user_config.json.
    Creates data/ if needed. Returns True on success.
    """
    path = _config_path(app_root)
    dir_path = os.path.dirname(path)
    try:
        os.makedirs(dir_path, exist_ok=True)
        data = {"default_k": k}
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except OSError as e:
        logger.warning("Could not save user config to %s: %s", path, e)
        return False
