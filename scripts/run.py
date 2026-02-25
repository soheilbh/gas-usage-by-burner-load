"""Run Streamlit app on port 8502. From repo root: python scripts/run.py"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(_REPO_ROOT)
sys.path.insert(0, _REPO_ROOT)

if __name__ == "__main__":
    import streamlit.web.cli as stcli
    sys.argv = ["streamlit", "run", "app.py", "--server.port=8502"]
    sys.exit(stcli.main())
