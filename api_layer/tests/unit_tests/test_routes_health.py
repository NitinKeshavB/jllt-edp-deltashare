"""Tests for health check endpoints."""

from datetime import datetime


class TestHealthEndpointsNoAuthRequired:
    """Tests verifying health endpoints work without authentication."""

    def test_health_check_no_auth_required(self, unauthenticated_client):
        """Test that /health endpoint works without authentication headers."""
        response = unauthenticated_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_liveness_no_auth_required(self, unauthenticated_client):
        """Test that /health/live endpoint works without authentication headers."""
        response = unauthenticated_client.get("/health/live")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"

    def test_readiness_no_auth_required(self, unauthenticated_client):
        """Test that /health/ready endpoint works without authentication headers."""
        response = unauthenticated_client.get("/health/ready")

        # Status can be 200 or 503 depending on config, but should not be 422
        assert response.status_code in [200, 503]

    def test_openapi_no_auth_required(self, unauthenticated_client):
        """Test that /openapi.json endpoint works without authentication headers."""
        response = unauthenticated_client.get("/openapi.json")

        assert response.status_code == 200


def test_health_check(client):
    """Test basic health check endpoint."""
    response = client.get("/health")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "healthy"
    assert data["service"] == "Delta Share API"
    assert data["version"] == "v1"
    assert "timestamp" in data
    assert "workspace_url" in data

    # Verify timestamp is a valid ISO format
    datetime.fromisoformat(data["timestamp"])


def test_liveness_check(client):
    """Test liveness check endpoint."""
    response = client.get("/health/live")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "alive"
    assert "timestamp" in data

    # Verify timestamp is a valid ISO format
    datetime.fromisoformat(data["timestamp"])


def test_readiness_check_success(client):
    """Test readiness check endpoint when app is ready."""
    response = client.get("/health/ready")

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ready"
    assert data["service"] == "Delta Share API"
    assert "timestamp" in data
    assert "checks" in data

    # Verify all checks passed
    checks = data["checks"]
    assert checks["settings"] == "ok"
    assert checks["authentication"] == "ok"

    # Should not have error field when ready
    assert "error" not in data


def test_readiness_check_missing_workspace_url(app, client):
    """Test readiness check fails when workspace URL is missing."""
    # Temporarily remove workspace URL
    original_url = app.state.settings.dltshr_workspace_url
    app.state.settings.dltshr_workspace_url = ""

    try:
        response = client.get("/health/ready")

        assert response.status_code == 503
        data = response.json()

        assert data["status"] == "not_ready"
        assert data["checks"]["settings"] == "failed"
        assert "error" in data
        assert "Workspace URL" in data["error"]

    finally:
        # Restore original value
        app.state.settings.dltshr_workspace_url = original_url


def test_readiness_check_missing_credentials(app, client):
    """Test readiness check fails when authentication credentials are missing."""
    # Temporarily remove credentials
    original_client_id = app.state.settings.client_id
    app.state.settings.client_id = ""

    try:
        response = client.get("/health/ready")

        assert response.status_code == 503
        data = response.json()

        assert data["status"] == "not_ready"
        assert data["checks"]["authentication"] == "failed"
        assert "error" in data
        assert "Authentication credentials" in data["error"]

    finally:
        # Restore original value
        app.state.settings.client_id = original_client_id


def test_health_endpoints_in_openapi_docs(client):
    """Test that health check endpoints appear in OpenAPI documentation."""
    response = client.get("/openapi.json")

    assert response.status_code == 200
    openapi_spec = response.json()

    # Verify health endpoints are documented
    assert "/health" in openapi_spec["paths"]
    assert "/health/ready" in openapi_spec["paths"]
    assert "/health/live" in openapi_spec["paths"]

    # Verify they're tagged correctly
    health_endpoint = openapi_spec["paths"]["/health"]["get"]
    assert "Health" in health_endpoint["tags"]
