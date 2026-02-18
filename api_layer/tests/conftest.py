"""
Register pytest plugins, fixtures, and hooks to be used during test execution.

All fixtures are organized in the fixtures/ directory for better maintainability.

Docs: https://stackoverflow.com/questions/34466027/in-pytest-what-is-the-use-of-conftest-py-files
"""

import sys
from pathlib import Path

THIS_DIR = Path(__file__).parent
TESTS_DIR_PARENT = (THIS_DIR / "..").resolve()

# add the parent directory of tests/ to PYTHONPATH
# so that we can use "from tests.<module> import ..." in our tests and fixtures
sys.path.insert(0, str(TESTS_DIR_PARENT))

# Register all fixture modules
# Fixtures are automatically discovered from these modules
pytest_plugins = [
    # Core application fixtures
    "tests.fixtures.app_fixtures",
    # Databricks SDK mocks
    "tests.fixtures.databricks_fixtures",
    # Pipeline fixtures
    "tests.fixtures.pipeline_fixtures",
    # Schedule fixtures
    "tests.fixtures.schedule_fixtures",
    # Azure service mocks
    "tests.fixtures.azure_fixtures",
    # Logging mocks
    "tests.fixtures.logging_fixtures",
    # Business logic mocks
    "tests.fixtures.business_logic_fixtures",
    # Catalog API fixtures
    "tests.fixtures.catalog_fixtures",
    # Workflow API fixtures
    "tests.fixtures.workflow_fixtures",
    # Example fixture (can be removed if not needed)
    "tests.fixtures.example_fixture",
]
