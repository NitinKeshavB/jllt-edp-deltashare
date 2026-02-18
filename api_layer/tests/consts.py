"""Constant values used for tests."""

from pathlib import Path

THIS_DIR = Path(__file__).parent
PROJECT_DIR = (THIS_DIR / "../").resolve()

# Base path for all API routes; must match main.py include_router(..., prefix="/api")
API_BASE = "/api"
