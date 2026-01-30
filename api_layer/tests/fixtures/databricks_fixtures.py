"""Fixtures for Databricks SDK mocks."""

from datetime import datetime
from datetime import timezone
from typing import List
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service.sharing import AuthenticationType
from databricks.sdk.service.sharing import IpAccessList
from databricks.sdk.service.sharing import RecipientInfo
from databricks.sdk.service.sharing import RecipientTokenInfo
from databricks.sdk.service.sharing import ShareInfo
from databricks.sdk.service.sharing import ShareToPrivilegeAssignment


@pytest.fixture
def mock_auth_token():
    """Mock the get_auth_token function."""
    with patch("dbrx_api.dbrx_auth.token_gen.get_auth_token") as mock:
        mock.return_value = ("test-auth-token", datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
        yield mock


@pytest.fixture
def mock_workspace_reachable():
    """Mock the check_workspace_reachable function to always return True in tests."""
    with patch("dbrx_api.dependencies.check_workspace_reachable") as mock:
        mock.return_value = (True, None)
        yield mock


@pytest.fixture
def mock_share_info():
    """Create a mock ShareInfo object."""

    def _create_share(
        name: str = "test_share",
        owner: str = "test_owner",
        comment: str = "Test share",
        created_at: int | None = None,
        updated_at: int | None = None,
    ) -> ShareInfo:
        """Factory function to create ShareInfo instances."""
        created = created_at or int(datetime.now(timezone.utc).timestamp() * 1000)
        updated = updated_at or int(datetime.now(timezone.utc).timestamp() * 1000)

        return ShareInfo(
            name=name,
            owner=owner,
            comment=comment,
            created_at=created,
            updated_at=updated,
        )

    return _create_share


@pytest.fixture
def mock_recipient_info():
    """Create a mock RecipientInfo object."""

    def _create_recipient(
        name: str = "test_recipient",
        owner: str = "test_owner",
        auth_type: AuthenticationType = AuthenticationType.TOKEN,
        comment: str = "Test recipient",
        created_at: int | None = None,
        updated_at: int | None = None,
        sharing_code: str | None = None,
        tokens: List[RecipientTokenInfo] | None = None,
        ip_access_list: IpAccessList | None = None,
    ) -> RecipientInfo:
        """Factory function to create RecipientInfo instances."""
        created = created_at or int(datetime.now(timezone.utc).timestamp() * 1000)
        updated = updated_at or int(datetime.now(timezone.utc).timestamp() * 1000)

        return RecipientInfo(
            name=name,
            owner=owner,
            authentication_type=auth_type,
            comment=comment,
            created_at=created,
            updated_at=updated,
            sharing_code=sharing_code,
            tokens=tokens,
            ip_access_list=ip_access_list,
        )

    return _create_recipient


@pytest.fixture
def mock_workspace_client(mock_share_info, mock_recipient_info):
    """Mock Databricks WorkspaceClient."""
    with patch("databricks.sdk.WorkspaceClient") as mock_client_class:
        # Create mock instance
        mock_client = MagicMock()

        # Mock shares API
        mock_shares_api = MagicMock()
        mock_shares_api.list.return_value = [
            mock_share_info(name="share1"),
            mock_share_info(name="share2"),
        ]
        mock_shares_api.get.return_value = mock_share_info(name="test_share")
        mock_shares_api.create.return_value = mock_share_info(name="new_share")
        mock_shares_api.delete.return_value = None
        mock_shares_api.update.return_value = mock_share_info(name="updated_share")

        # Mock recipients API
        mock_recipients_api = MagicMock()
        mock_recipients_api.list.return_value = [
            mock_recipient_info(name="recipient1"),
            mock_recipient_info(name="recipient2"),
        ]
        mock_recipients_api.get.return_value = mock_recipient_info(name="test_recipient")
        mock_recipients_api.create.return_value = mock_recipient_info(name="new_recipient")
        mock_recipients_api.delete.return_value = None
        mock_recipients_api.update.return_value = mock_recipient_info(name="updated_recipient")
        mock_recipients_api.rotate_token.return_value = mock_recipient_info(
            name="test_recipient",
            tokens=[
                RecipientTokenInfo(
                    id="token_id_1",
                    activation_url="https://test-activation.databricks.com",
                    created_at=int(datetime.now(timezone.utc).timestamp() * 1000),
                )
            ],
        )

        # Mock share permissions API
        mock_permissions_api = MagicMock()
        mock_permissions_api.get_share_permissions.return_value = {
            "privilege_assignments": [
                ShareToPrivilegeAssignment(
                    principal="test_recipient",
                    privileges=["SELECT"],
                )
            ]
        }
        mock_permissions_api.update_share_permissions.return_value = None

        # Attach mocked APIs to client
        mock_client.shares = mock_shares_api
        mock_client.recipients = mock_recipients_api
        mock_client.grants = mock_permissions_api

        # Make the class return our mock instance
        mock_client_class.return_value = mock_client

        yield mock_client
