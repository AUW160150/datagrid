"""
datagrid — Pipeline Entry Point

Usage:
  python run_pipeline.py            # run full pipeline
  python run_pipeline.py --force    # clear Ghost cache and rerun
"""

import sys
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

sys.path.insert(0, os.path.dirname(__file__))

from pipeline.orchestrator import run

if __name__ == "__main__":
    force   = "--force" in sys.argv
    verbose = "--quiet" not in sys.argv
    run(force_rerun=force, verbose=verbose)
