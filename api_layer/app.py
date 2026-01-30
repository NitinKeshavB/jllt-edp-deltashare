"""WSGI/ASGI entry point for Azure Web App deployment.

Azure Web App looks for an 'app' variable in the application module.
This module provides the FastAPI application instance.

Usage:
    - Azure: uvicorn app:app --host 0.0.0.0 --port 8000
    - Local: uvicorn app:app --reload
"""

import sys
from pathlib import Path

# Add src to Python path for imports (MUST be before importing dbrx_api)
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Now import and create the app
from dbrx_api.main import create_app

# Azure Web App expects 'app' variable
app = create_app()
