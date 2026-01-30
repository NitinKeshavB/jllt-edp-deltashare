"""Unit tests for dependencies.py."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from dbrx_api.dependencies import check_workspace_reachable
from dbrx_api.dependencies import get_settings
from dbrx_api.dependencies import get_token_manager
from dbrx_api.dependencies import get_workspace_url
from dbrx_api.dependencies import is_valid_databricks_url
from dbrx_api.dependencies import verify_apim_request


class TestIsValidDatabricksUrl:
    """Tests for is_valid_databricks_url function."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            # Valid URLs
            ("https://adb-123456789.12.azuredatabricks.net", True),
            ("https://adb-123456789.12.azuredatabricks.net/", True),
            ("https://myworkspace.cloud.databricks.com", True),
            ("https://myworkspace.gcp.databricks.com", True),
            # Invalid URLs
            ("http://adb-123456789.12.azuredatabricks.net", False),
            ("https://example.com", False),
            ("", False),
            ("https://invalid-domain.com", False),
            ("ftp://adb-123.azuredatabricks.net", False),
        ],
        ids=[
            "azure_valid",
            "azure_trailing_slash",
            "aws_valid",
            "gcp_valid",
            "http_invalid",
            "wrong_domain",
            "empty_string",
            "invalid_domain",
            "wrong_protocol",
        ],
    )
    def test_url_validation(self, url: str, expected: bool):
        """Test URL validation for various inputs."""
        assert is_valid_databricks_url(url) is expected


class TestCheckWorkspaceReachable:
    """Tests for check_workspace_reachable function."""

    @pytest.mark.asyncio
    @patch("dbrx_api.dependencies.socket.gethostbyname")
    @patch("dbrx_api.dependencies.httpx.AsyncClient")
    async def test_workspace_reachable(self, mock_client_class, mock_dns):
        """Test workspace is reachable."""
        mock_dns.return_value = "1.2.3.4"

        mock_response = MagicMock()
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.head = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        is_reachable, error = await check_workspace_reachable("https://adb-123.12.azuredatabricks.net")

        assert is_reachable is True
        assert error == ""

    @pytest.mark.asyncio
    @patch("dbrx_api.dependencies.socket.gethostbyname")
    async def test_workspace_dns_failure(self, mock_dns):
        """Test workspace DNS resolution failure."""
        import socket

        mock_dns.side_effect = socket.gaierror("DNS resolution failed")

        is_reachable, error = await check_workspace_reachable("https://nonexistent.azuredatabricks.net")

        assert is_reachable is False
        assert "could not be resolved" in error

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception,expected_error_substring",
        [
            (httpx.TimeoutException("Timeout"), "timed out"),
            (httpx.ConnectError("connection refused"), "refused"),
            (httpx.ConnectError("name or service not known"), "could not be resolved"),
        ],
        ids=["timeout", "connection_refused", "dns_error_in_http"],
    )
    @patch("dbrx_api.dependencies.socket.gethostbyname")
    @patch("dbrx_api.dependencies.httpx.AsyncClient")
    async def test_workspace_connection_errors(self, mock_client_class, mock_dns, exception, expected_error_substring):
        """Test various workspace connection error scenarios."""
        mock_dns.return_value = "1.2.3.4"

        mock_client = AsyncMock()
        mock_client.head = AsyncMock(side_effect=exception)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        is_reachable, error = await check_workspace_reachable("https://adb-123.12.azuredatabricks.net")

        assert is_reachable is False
        assert expected_error_substring in error


class TestGetWorkspaceUrl:
    """Tests for get_workspace_url dependency."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url,expected_status,expected_detail_substring",
        [
            ("", 400, "required"),
            ("   ", 400, "required"),
            ("http://adb-123.12.azuredatabricks.net", 400, "HTTPS"),
            ("https://example.com", 400, "Invalid Databricks workspace URL"),
        ],
        ids=["empty", "whitespace", "http_not_https", "invalid_domain"],
    )
    async def test_invalid_urls(self, url: str, expected_status: int, expected_detail_substring: str):
        """Test various invalid URL inputs."""
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await get_workspace_url(url)

        assert exc_info.value.status_code == expected_status
        assert expected_detail_substring in exc_info.value.detail

    @pytest.mark.asyncio
    @patch("dbrx_api.dependencies.check_workspace_reachable")
    async def test_unreachable_workspace(self, mock_check):
        """Test unreachable workspace returns 502."""
        from fastapi import HTTPException

        mock_check.return_value = (False, "Connection failed")

        with pytest.raises(HTTPException) as exc_info:
            await get_workspace_url("https://adb-123.12.azuredatabricks.net")

        assert exc_info.value.status_code == 502

    @pytest.mark.asyncio
    @patch("dbrx_api.dependencies.check_workspace_reachable")
    async def test_valid_url_success(self, mock_check):
        """Test valid URL returns normalized URL."""
        mock_check.return_value = (True, "")

        result = await get_workspace_url("https://adb-123.12.azuredatabricks.net/")

        # Should strip trailing slash
        assert result == "https://adb-123.12.azuredatabricks.net"


class TestGetSettings:
    """Tests for get_settings dependency."""

    def test_get_settings_from_request(self):
        """Test getting settings from request state."""
        mock_settings = MagicMock()
        mock_request = MagicMock()
        mock_request.app.state.settings = mock_settings

        result = get_settings(mock_request)

        assert result == mock_settings


class TestGetTokenManager:
    """Tests for get_token_manager dependency."""

    def test_get_token_manager_from_request(self):
        """Test getting token manager from request state."""
        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.app.state.token_manager = mock_manager

        result = get_token_manager(mock_request)

        assert result == mock_manager


class TestVerifyApimRequest:
    """Tests for verify_apim_request dependency."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "header_value,expected",
        [
            ("some_value", True),
            ("", True),  # Empty string is still "present"
            (None, False),
        ],
        ids=["present", "empty_string", "absent"],
    )
    async def test_apim_header_detection(self, header_value, expected: bool):
        """Test APIM header presence detection."""
        mock_request = MagicMock()
        result = await verify_apim_request(mock_request, header_value)
        assert result is expected
