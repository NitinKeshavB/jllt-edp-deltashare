"""Unit tests for errors.py error handlers."""

import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pydantic
import pytest
from databricks.sdk.errors import BadRequest
from databricks.sdk.errors import DatabricksError
from databricks.sdk.errors import NotFound
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.errors import Unauthenticated
from fastapi import Request

from dbrx_api.errors import handle_broad_exceptions
from dbrx_api.errors import handle_databricks_connection_error
from dbrx_api.errors import handle_databricks_errors
from dbrx_api.errors import handle_pydantic_validation_errors


class TestHandleBroadExceptions:
    """Tests for handle_broad_exceptions middleware."""

    @pytest.mark.asyncio
    @patch("dbrx_api.errors.log_response_info")
    async def test_successful_request(self, mock_log):
        """Test middleware passes through successful requests."""
        mock_request = MagicMock(spec=Request)
        mock_response = MagicMock()

        async def mock_call_next(request):
            return mock_response

        result = await handle_broad_exceptions(mock_request, mock_call_next)

        assert result == mock_response
        mock_log.assert_not_called()

    @pytest.mark.asyncio
    @patch("dbrx_api.errors.log_response_info")
    async def test_exception_returns_500(self, mock_log):
        """Test middleware catches exceptions and returns 500."""
        mock_request = MagicMock(spec=Request)
        mock_request.state.request_body = None  # Avoid MagicMock in json.dumps

        async def mock_call_next(request):
            raise ValueError("Test error")

        result = await handle_broad_exceptions(mock_request, mock_call_next)

        assert result.status_code == 500
        mock_log.assert_called_once()


class TestHandlePydanticValidationErrors:
    """Tests for handle_pydantic_validation_errors handler."""

    @pytest.mark.asyncio
    @patch("dbrx_api.errors.log_response_info")
    async def test_validation_error(self, mock_log):
        """Test handling pydantic validation errors."""
        mock_request = MagicMock(spec=Request)
        mock_request.state.request_body = None  # Avoid MagicMock in json.dumps

        class TestModel(pydantic.BaseModel):
            name: str
            value: int

        try:
            TestModel(name=123, value="not_int")
        except pydantic.ValidationError as exc:
            result = await handle_pydantic_validation_errors(mock_request, exc)

            assert result.status_code == 422
            mock_log.assert_called_once()


class TestHandleDatabricksErrors:
    """Tests for handle_databricks_errors handler."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception_class,message,expected_status",
        [
            (Unauthenticated, "Auth failed", 401),
            (PermissionDenied, "Access denied", 403),
            (NotFound, "Resource not found", 404),
            (BadRequest, "Invalid request", 400),
            (DatabricksError, "Generic error", 502),
        ],
        ids=["unauthenticated", "permission_denied", "not_found", "bad_request", "generic_error"],
    )
    @patch("dbrx_api.errors.log_response_info")
    async def test_databricks_error_mapping(self, mock_log, exception_class, message: str, expected_status: int):
        """Test Databricks errors are mapped to correct HTTP status codes."""
        mock_request = MagicMock(spec=Request)
        mock_request.url.path = "/test"
        mock_request.state.request_body = None  # Avoid MagicMock in json.dumps

        exc = exception_class(message)
        result = await handle_databricks_errors(mock_request, exc)

        assert result.status_code == expected_status
        mock_log.assert_called_once()


class TestHandleDatabricksConnectionError:
    """Tests for handle_databricks_connection_error function."""

    @pytest.mark.parametrize(
        "error_message,expected_detail_substring",
        [
            ("Connection timeout", "timed out"),
            ("name or service not known", "resolve"),
            ("nodename nor servname provided", "resolve"),
            ("connection refused", "refused"),
            ("SSL certificate verify failed", "SSL"),
            ("certificate error", "SSL"),
            ("Some other error", "Unable to connect"),
        ],
        ids=[
            "timeout",
            "dns_name_not_known",
            "dns_nodename",
            "connection_refused",
            "ssl_verify_failed",
            "certificate_error",
            "generic_error",
        ],
    )
    def test_connection_error_messages(self, error_message: str, expected_detail_substring: str):
        """Test various connection error messages produce correct responses."""
        error = Exception(error_message)
        result = handle_databricks_connection_error(error)

        assert result.status_code == 503
        content = json.loads(result.body)
        assert expected_detail_substring in content["detail"]
        assert content["error_type"] == "ConnectionError"
