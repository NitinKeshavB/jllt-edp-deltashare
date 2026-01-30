"""Unit tests for dbrx_auth/token_manager.py.

Tests the in-memory token caching behavior of TokenManager.
Tokens are never read from or written to environment variables or files.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import patch

from dbrx_api.dbrx_auth.token_manager import TokenManager


class TestTokenManagerInit:
    """Tests for TokenManager initialization."""

    def test_init_creates_empty_cache(self):
        """Test initialization starts with no cached token."""
        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        assert manager.client_id == "test_client"
        assert manager.client_secret == "test_secret"
        assert manager.account_id == "test_account"
        assert manager.cached_token is None
        assert manager.cached_expiry is None

    def test_init_is_lazy(self):
        """Test that token is NOT generated at init time (lazy initialization)."""
        with patch("dbrx_api.dbrx_auth.token_manager.get_auth_token") as mock_get_auth:
            TokenManager(
                client_id="test_client",
                client_secret="test_secret",
                account_id="test_account",
            )

            # get_auth_token should NOT be called during init
            mock_get_auth.assert_not_called()


class TestTokenManagerGetToken:
    """Tests for TokenManager.get_token method."""

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_get_token_generates_on_first_call(self, mock_get_auth):
        """Test that first get_token() call generates a new token."""
        expected_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("new_token", expected_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        token, expiry = manager.get_token()

        assert token == "new_token"
        assert expiry == expected_expiry
        mock_get_auth.assert_called_once()

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_get_token_caches_in_memory(self, mock_get_auth):
        """Test that subsequent get_token() calls return cached token."""
        expected_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("generated_token", expected_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # First call - should generate
        token1, _ = manager.get_token()

        # Second call - should return cached (not generate again)
        token2, _ = manager.get_token()

        # Third call - should still return cached
        token3, _ = manager.get_token()

        assert token1 == token2 == token3 == "generated_token"
        # Only called ONCE despite 3 get_token() calls
        mock_get_auth.assert_called_once()

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_get_token_refreshes_when_expiring_soon(self, mock_get_auth):
        """Test that token is refreshed when expiring in less than 5 minutes."""
        # First call returns token expiring in 2 minutes (less than 5 min threshold)
        expiring_soon = datetime.now(timezone.utc) + timedelta(minutes=2)
        new_expiry = datetime.now(timezone.utc) + timedelta(hours=1)

        mock_get_auth.side_effect = [
            ("first_token", expiring_soon),
            ("refreshed_token", new_expiry),
        ]

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # First call - gets token expiring in 2 min
        token1, _ = manager.get_token()
        assert token1 == "first_token"

        # Second call - should refresh because token expires soon
        token2, _ = manager.get_token()
        assert token2 == "refreshed_token"

        # Called twice due to refresh
        assert mock_get_auth.call_count == 2

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_get_token_uses_cached_when_valid(self, mock_get_auth):
        """Test that valid token (>5 min until expiry) is reused."""
        # Token expires in 1 hour (well above 5 min threshold)
        valid_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("valid_token", valid_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # Multiple calls should all return cached token
        for _ in range(5):
            token, _ = manager.get_token()
            assert token == "valid_token"

        # Only one call to generate token
        mock_get_auth.assert_called_once()


class TestTokenManagerProperties:
    """Tests for TokenManager properties."""

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_cached_token_property(self, mock_get_auth):
        """Test cached_token property returns in-memory cached token."""
        expected_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("my_token", expected_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # Before first get_token(), cache should be empty
        assert manager.cached_token is None

        # After get_token(), cache should have the token
        manager.get_token()
        assert manager.cached_token == "my_token"

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_cached_expiry_property(self, mock_get_auth):
        """Test cached_expiry property returns in-memory cached expiry."""
        expected_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("my_token", expected_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # Before first get_token(), cache should be empty
        assert manager.cached_expiry is None

        # After get_token(), cache should have the expiry
        manager.get_token()
        assert manager.cached_expiry == expected_expiry


class TestTokenManagerIsTokenValid:
    """Tests for TokenManager.is_token_valid method."""

    def test_is_token_valid_no_token(self):
        """Test is_token_valid returns False when no token cached."""
        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        assert manager.is_token_valid() is False

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_is_token_valid_with_valid_token(self, mock_get_auth):
        """Test is_token_valid returns True when token expires in >5 minutes."""
        valid_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("valid_token", valid_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        manager.get_token()
        assert manager.is_token_valid() is True

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_is_token_valid_with_expiring_token(self, mock_get_auth):
        """Test is_token_valid returns False when token expires in <5 minutes."""
        expiring_soon = datetime.now(timezone.utc) + timedelta(minutes=2)
        mock_get_auth.return_value = ("expiring_token", expiring_soon)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        manager.get_token()
        assert manager.is_token_valid() is False


class TestTokenManagerInvalidate:
    """Tests for TokenManager.invalidate_token method."""

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_invalidate_token(self, mock_get_auth):
        """Test invalidating cached token clears the in-memory cache."""
        valid_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.return_value = ("my_token", valid_expiry)

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # Generate and cache token
        manager.get_token()
        assert manager.cached_token is not None

        # Invalidate should clear cache
        manager.invalidate_token()
        assert manager.cached_token is None
        assert manager.cached_expiry is None

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_invalidate_forces_new_generation(self, mock_get_auth):
        """Test that invalidate_token forces a new token generation."""
        valid_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_get_auth.side_effect = [
            ("first_token", valid_expiry),
            ("second_token", valid_expiry),
        ]

        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # First get_token
        token1, _ = manager.get_token()
        assert token1 == "first_token"
        assert mock_get_auth.call_count == 1

        # Invalidate
        manager.invalidate_token()

        # Next get_token should generate new token
        token2, _ = manager.get_token()
        assert token2 == "second_token"
        assert mock_get_auth.call_count == 2


class TestTokenManagerThreadSafety:
    """Tests for TokenManager thread safety."""

    @patch("dbrx_api.dbrx_auth.token_manager.get_auth_token")
    def test_has_lock(self, mock_get_auth):
        """Test that TokenManager uses a lock for thread safety."""
        manager = TokenManager(
            client_id="test_client",
            client_secret="test_secret",
            account_id="test_account",
        )

        # Verify lock exists
        assert hasattr(manager, "_lock")
        assert manager._lock is not None
