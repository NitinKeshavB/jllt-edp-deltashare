"""Tests for health check endpoints."""

from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

from tests.consts import API_BASE


class TestHealthEndpointsNoAuthRequired:
    """Tests verifying health endpoints work without authentication."""

    def test_health_check_no_auth_required(self, unauthenticated_client):
        """Test that /api/health endpoint works without authentication headers."""
        response = unauthenticated_client.get(f"{API_BASE}/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_openapi_no_auth_required(self, unauthenticated_client):
        """Test that /openapi.json endpoint works without authentication headers."""
        response = unauthenticated_client.get("/openapi.json")

        assert response.status_code == 200


def test_health_check(client):
    """Test basic health check endpoint."""
    response = client.get(f"{API_BASE}/health")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "healthy"
    assert data["service"] == "Delta Share API"
    assert data["version"] == "v1"
    assert "timestamp" in data
    assert "workspace_url" in data

    # Verify timestamp is a valid ISO format
    datetime.fromisoformat(data["timestamp"])


def test_health_logging_test_success(client):
    """POST /api/health/logging/test returns 200 when blob handler succeeds."""
    mock_handler = MagicMock()
    mock_handler.test_upload.return_value = {"success": True, "blob_name": "test.log"}

    with patch("dbrx_api.monitoring.logger._azure_blob_handler", mock_handler):
        response = client.post(f"{API_BASE}/health/logging/test")

    assert response.status_code == 200
    data = response.json()
    assert "timestamp" in data
    assert data.get("test_result", {}).get("success") is True


def test_health_logging_test_handler_not_initialized(client):
    """POST /api/health/logging/test returns 503 when blob handler not initialized."""
    with patch("dbrx_api.monitoring.logger._azure_blob_handler", None):
        response = client.post(f"{API_BASE}/health/logging/test")

    assert response.status_code == 503
    data = response.json()
    assert "error" in data or "message" in data


def test_health_logging_test_upload_failure(client):
    """POST /api/health/logging/test returns 500 when blob handler returns failure."""
    mock_handler = MagicMock()
    mock_handler.test_upload.return_value = {"success": False, "error": "Upload failed"}

    with patch("dbrx_api.monitoring.logger._azure_blob_handler", mock_handler):
        response = client.post(f"{API_BASE}/health/logging/test")

    assert response.status_code == 500
    data = response.json()
    assert "test_result" in data


def test_health_endpoints_in_openapi_docs(client):
    """Test that health check endpoints appear in OpenAPI documentation."""
    response = client.get("/openapi.json")

    assert response.status_code == 200
    openapi_spec = response.json()
    paths = openapi_spec.get("paths", {})

    # Verify health endpoints are documented (paths may include API_BASE)
    assert f"{API_BASE}/health" in paths
    assert f"{API_BASE}/health/logging/test" in paths

    # Verify main health endpoint is tagged correctly
    health_get = paths[f"{API_BASE}/health"].get("get")
    assert health_get is not None
    assert "Health" in health_get.get("tags", [])
