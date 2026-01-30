"""Fixtures for FastAPI application and settings."""

import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from starlette.testclient import TestClient as StarletteTestClient

# Ensure tests can import from parent directory
THIS_DIR = Path(__file__).parent
TESTS_DIR = THIS_DIR.parent
TESTS_DIR_PARENT = (TESTS_DIR / "..").resolve()
sys.path.insert(0, str(TESTS_DIR_PARENT))

# Default headers required for API authentication
DEFAULT_TEST_HEADERS = {
    "X-Workspace-URL": "https://test-workspace.azuredatabricks.net/",
}


class AuthenticatedTestClient(StarletteTestClient):
    """Test client that automatically includes required authentication headers."""

    def __init__(self, *args: Any, default_headers: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """Initialize with default headers."""
        super().__init__(*args, **kwargs)
        self._default_headers = default_headers or DEFAULT_TEST_HEADERS

    def _merge_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Merge default headers with provided headers."""
        merged = dict(self._default_headers)
        if headers:
            merged.update(headers)
        return merged

    def get(self, url: str, **kwargs: Any) -> Any:
        """GET request with default headers."""
        kwargs["headers"] = self._merge_headers(kwargs.get("headers"))
        return super().get(url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        """POST request with default headers."""
        kwargs["headers"] = self._merge_headers(kwargs.get("headers"))
        return super().post(url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Any:
        """PUT request with default headers."""
        kwargs["headers"] = self._merge_headers(kwargs.get("headers"))
        return super().put(url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Any:
        """DELETE request with default headers."""
        kwargs["headers"] = self._merge_headers(kwargs.get("headers"))
        return super().delete(url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> Any:
        """PATCH request with default headers."""
        kwargs["headers"] = self._merge_headers(kwargs.get("headers"))
        return super().patch(url, **kwargs)


@pytest.fixture
def mock_settings():
    """Mock Settings object with test configuration."""
    from dbrx_api.settings import Settings

    with patch.dict(
        "os.environ",
        {
            "DLTSHR_WORKSPACE_URL": "https://test-workspace.azuredatabricks.net/",
            "CLIENT_ID": "test-client-id",
            "CLIENT_SECRET": "test-client-secret",
            "ACCOUNT_ID": "test-account-id",
            "ENABLE_BLOB_LOGGING": "false",
            "ENABLE_POSTGRESQL_LOGGING": "false",
        },
    ):
        settings = Settings()
        yield settings


@pytest.fixture
def mock_get_auth_token():
    """Mock get_auth_token function that returns a test token."""
    test_token = "test-databricks-token"
    test_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    return (test_token, test_expiry)


@pytest.fixture
def app(mock_settings):
    """Create FastAPI test application with mocked settings."""
    from dbrx_api.main import create_app

    # Create app with mocked settings
    app = create_app(settings=mock_settings)
    yield app


@pytest.fixture
def client(app):
    """Create FastAPI test client with default authentication headers and mocked workspace validation."""
    # Mock workspace reachability check to always succeed in tests
    with patch("dbrx_api.dependencies.check_workspace_reachable") as mock_reachable:
        mock_reachable.return_value = (True, None)

        with AuthenticatedTestClient(app) as test_client:
            yield test_client


@pytest.fixture
def unauthenticated_client(app):
    """Create FastAPI test client without authentication headers (for testing auth failures)."""
    with TestClient(app) as test_client:
        yield test_client
