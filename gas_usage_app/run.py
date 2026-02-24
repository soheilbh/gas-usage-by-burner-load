"""Entry point to run the Streamlit app. Run from repo root: python gas_usage_app/run.py"""
import os
import sys

# Ensure repo root is on path and cwd when running from any location
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(_REPO_ROOT)
sys.path.insert(0, _REPO_ROOT)

if __name__ == "__main__":
    import streamlit.web.cli as stcli
    sys.argv = ["streamlit", "run", "gas_usage_app/app.py", "--server.port=8502"]
    sys.exit(stcli.main())
