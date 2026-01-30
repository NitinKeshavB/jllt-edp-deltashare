#!/bin/bash

# Azure Web App startup script for DeltaShare API
# Usage: ./startup.sh (from api_layer directory)

echo "=========================================="
echo "Starting DeltaShare API..."
echo "=========================================="

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Add src directory to PYTHONPATH for proper package imports
export PYTHONPATH="${SCRIPT_DIR}/src:${PYTHONPATH}"

echo "Working directory: $(pwd)"
echo "PYTHONPATH: $PYTHONPATH"
echo "Python version: $(python --version 2>&1)"

# Check if .env file exists (local development)
if [ -f ".env" ]; then
    echo "Environment: Local (.env file found)"
else
    echo "Environment: Azure Web App (using App Settings)"
fi

echo "=========================================="

# Start the application
# app:app refers to app.py file and 'app' variable
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 2
