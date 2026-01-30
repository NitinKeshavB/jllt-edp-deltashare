"""Unit tests for dbrx_auth/token_gen.py.

NOTE: Token caching is now handled by TokenManager (tested in test_token_manager.py).
These tests focus on the token generation logic only.
"""

import json
import os
from datetime import datetime
from datetime import timezone
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import requests

from dbrx_api.dbrx_auth.token_gen import CustomError
from dbrx_api.dbrx_auth.token_gen import get_auth_token


class TestGetAuthToken:
    """Tests for get_auth_token function."""

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_success(self, mock_post):
        """Test generates new token successfully."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_token",
                "expires_in": 3600,
            }
            mock_post.return_value = mock_response

            token, expires_at = get_auth_token(exec_time)

            assert token == "new_token"
            assert expires_at > exec_time
            mock_post.assert_called_once()

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    @patch("dbrx_api.dbrx_auth.token_gen.Settings")
    def test_get_auth_token_missing_credentials(self, mock_settings_class, mock_post):
        """Test raises error when credentials missing."""
        from pydantic import ValidationError

        exec_time = datetime.now(timezone.utc)

        # Mock Settings to raise ValidationError when credentials are missing
        mock_settings_class.side_effect = ValidationError.from_exception_data(
            "ValueError",
            [
                {
                    "type": "missing",
                    "loc": ("client_id",),
                    "msg": "Field required",
                    "input": {},
                }
            ],
        )

        with pytest.raises(CustomError) as exc_info:
            get_auth_token(exec_time)

        assert "Missing required environment variables" in str(exc_info.value)

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_request_fails(self, mock_post):
        """Test raises error when token request fails."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_post.return_value = mock_response

            with pytest.raises(CustomError) as exc_info:
                get_auth_token(exec_time)

            assert "Token request failed" in str(exc_info.value)

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_invalid_json(self, mock_post):
        """Test raises error when response is not valid JSON."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", doc="", pos=0)
            mock_post.return_value = mock_response

            with pytest.raises(CustomError) as exc_info:
                get_auth_token(exec_time)

            assert "Failed to parse" in str(exc_info.value)

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_no_access_token_in_response(self, mock_post):
        """Test raises error when access_token not in response."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"expires_in": 3600}
            mock_post.return_value = mock_response

            with pytest.raises(CustomError) as exc_info:
                get_auth_token(exec_time)

            assert "Access token not found" in str(exc_info.value)

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_network_error(self, mock_post):
        """Test raises error on network failure."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_post.side_effect = requests.exceptions.ConnectionError("Network error")

            with pytest.raises(CustomError) as exc_info:
                get_auth_token(exec_time)

            assert "Network error" in str(exc_info.value)

    @patch("dbrx_api.dbrx_auth.token_gen.requests.post")
    def test_get_auth_token_default_expiry(self, mock_post):
        """Test uses default expiry when not in response."""
        exec_time = datetime.now(timezone.utc)

        with patch.dict(
            os.environ,
            {
                "CLIENT_ID": "test_client",
                "CLIENT_SECRET": "test_secret",
                "ACCOUNT_ID": "test_account",
            },
        ):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_token",
                # No expires_in - should default to 3600
            }
            mock_post.return_value = mock_response

            token, expires_at = get_auth_token(exec_time)

            assert token == "new_token"
            # Default is 3600 seconds (1 hour)
            assert (expires_at - exec_time).total_seconds() >= 3500  # Allow some margin


class TestCustomError:
    """Tests for CustomError exception."""

    def test_custom_error_message(self):
        """Test CustomError stores message correctly."""
        error = CustomError("Test error message")
        assert str(error) == "Test error message"

    def test_custom_error_inheritance(self):
        """Test CustomError inherits from Exception."""
        error = CustomError("Test")
        assert isinstance(error, Exception)
