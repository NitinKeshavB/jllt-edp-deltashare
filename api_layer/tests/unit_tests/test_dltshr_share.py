"""Unit tests for dltshr/share.py business logic."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service.sharing import ShareInfo

from dbrx_api.dltshr.share import add_data_object_to_share
from dbrx_api.dltshr.share import add_recipients_to_share
from dbrx_api.dltshr.share import create_share
from dbrx_api.dltshr.share import delete_share
from dbrx_api.dltshr.share import get_shares
from dbrx_api.dltshr.share import list_shares_all
from dbrx_api.dltshr.share import remove_recipients_from_share
from dbrx_api.dltshr.share import revoke_data_object_from_share


class TestListSharesAll:
    """Tests for list_shares_all function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_list_shares_all_success(self, mock_auth, mock_client_class):
        """Test successful listing of all shares."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share1 = MagicMock(spec=ShareInfo)
        mock_share1.name = "share1"
        mock_share2 = MagicMock(spec=ShareInfo)
        mock_share2.name = "share2"
        mock_client.shares.list_shares.return_value = [mock_share1, mock_share2]

        result = list_shares_all("https://test.azuredatabricks.net")

        assert len(result) == 2
        mock_client.shares.list_shares.assert_called_once_with(max_results=100)

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_list_shares_all_with_prefix(self, mock_auth, mock_client_class):
        """Test listing shares with prefix filter."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share1 = MagicMock(spec=ShareInfo)
        mock_share1.name = "test_share1"
        mock_share2 = MagicMock(spec=ShareInfo)
        mock_share2.name = "other_share"
        mock_client.shares.list_shares.return_value = [mock_share1, mock_share2]

        result = list_shares_all("https://test.azuredatabricks.net", prefix="test")

        assert len(result) == 1
        assert result[0].name == "test_share1"

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_list_shares_all_empty(self, mock_auth, mock_client_class):
        """Test listing shares when none exist."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.list_shares.return_value = []

        result = list_shares_all("https://test.azuredatabricks.net")

        assert len(result) == 0


class TestGetShares:
    """Tests for get_shares function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_get_shares_success(self, mock_auth, mock_client_class):
        """Test successful retrieval of a share."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_share.name = "test_share"
        mock_share.owner = "test_owner"
        mock_client.shares.get.return_value = mock_share

        result = get_shares("test_share", "https://test.azuredatabricks.net")

        assert result is not None
        assert result.name == "test_share"
        mock_client.shares.get.assert_called_once_with(name="test_share")

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_get_shares_not_found(self, mock_auth, mock_client_class):
        """Test retrieval when share doesn't exist."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.get.side_effect = Exception("Share does not exist")

        result = get_shares("nonexistent", "https://test.azuredatabricks.net")

        assert result is None


class TestCreateShare:
    """Tests for create_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_create_share_success(self, mock_auth, mock_client_class):
        """Test successful share creation."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_share.name = "new_share"
        mock_client.shares.create.return_value = mock_share

        result = create_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="new_share",
            description="Test description",
        )

        assert result.name == "new_share"
        mock_client.shares.create.assert_called_once()

    @pytest.mark.parametrize(
        "error_message,expected_substring",
        [
            ("Share already exists", "already exists"),
            ("AlreadyExists: share exists", "already exists"),
            ("PERMISSION_DENIED", "Permission denied"),
            ("PermissionDenied: no access", "Permission denied"),
            ("INVALID_PARAMETER_VALUE: invalid name", "Invalid parameter"),
            ("RESOURCE_DOES_NOT_EXIST", "Storage root"),
            ("INVALID_STATE cannot proceed", "INVALID_STATE"),
        ],
        ids=[
            "already_exists",
            "already_exists_alt",
            "permission_denied",
            "permission_denied_alt",
            "invalid_parameter",
            "storage_not_found",
            "invalid_state",
        ],
    )
    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_create_share_errors(self, mock_auth, mock_client_class, error_message: str, expected_substring: str):
        """Test share creation error handling for various error scenarios."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.create.side_effect = Exception(error_message)

        result = create_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            description="Test description",
        )

        assert isinstance(result, str)
        assert expected_substring in result


class TestDeleteShare:
    """Tests for delete_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_delete_share_success(self, mock_auth, mock_client_class):
        """Test successful share deletion."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.delete.return_value = None

        result = delete_share("test_share", "https://test.azuredatabricks.net")

        assert result is None
        mock_client.shares.delete.assert_called_once_with(name="test_share")

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_delete_share_permission_denied(self, mock_auth, mock_client_class):
        """Test share deletion with permission denied."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.delete.side_effect = Exception("User is not an owner of Share")

        result = delete_share("test_share", "https://test.azuredatabricks.net")

        assert result == "User is not an owner of Share"

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_delete_share_not_found(self, mock_auth, mock_client_class):
        """Test share deletion when share doesn't exist."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.delete.side_effect = Exception("RESOURCE_DOES_NOT_EXIST")

        result = delete_share("nonexistent", "https://test.azuredatabricks.net")

        assert "not found" in result


class TestAddDataObjectToShare:
    """Tests for add_data_object_to_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_success(self, mock_auth, mock_client_class):
        """Test successful addition of data objects."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_client.shares.update.return_value = mock_share

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={"tables": ["catalog.schema.table1"]},
        )

        assert result is not None
        mock_client.shares.update.assert_called_once()

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_empty(self, mock_auth, mock_client_class):
        """Test adding empty objects list."""
        mock_auth.return_value = ("test_token", 3600)

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={},
        )

        assert result == "No data objects provided to add to share."

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_none(self, mock_auth, mock_client_class):
        """Test adding None objects list."""
        mock_auth.return_value = ("test_token", 3600)

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add=None,
        )

        assert result == "No data objects provided to add to share."

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_already_exists(self, mock_auth, mock_client_class):
        """Test adding object that already exists."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.update.side_effect = Exception("ResourceAlreadyExists")

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={"tables": ["catalog.schema.table1"]},
        )

        assert "already exists" in result

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_with_views(self, mock_auth, mock_client_class):
        """Test adding views to share."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_client.shares.update.return_value = mock_share

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={"views": ["catalog.schema.view1"]},
        )

        assert result is not None

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_with_schemas(self, mock_auth, mock_client_class):
        """Test adding schemas to share."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_client.shares.update.return_value = mock_share

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={"schemas": ["catalog.schema"]},
        )

        assert result is not None

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_data_objects_schema_conflict(self, mock_auth, mock_client_class):
        """Test adding schema that conflicts with individual tables."""
        mock_auth.return_value = ("test_token", 3600)

        result = add_data_object_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_add={
                "tables": ["catalog.schema.table1"],
                "schemas": ["catalog.schema"],
            },
        )

        assert "Cannot add schemas" in result


class TestRevokeDataObjectFromShare:
    """Tests for revoke_data_object_from_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_revoke_data_objects_success(self, mock_auth, mock_client_class):
        """Test successful revocation of data objects."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_share = MagicMock(spec=ShareInfo)
        mock_client.shares.update.return_value = mock_share

        result = revoke_data_object_from_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_revoke={"tables": ["catalog.schema.table1"]},
        )

        assert result is not None

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_revoke_data_objects_empty(self, mock_auth, mock_client_class):
        """Test revoking empty objects list."""
        mock_auth.return_value = ("test_token", 3600)

        result = revoke_data_object_from_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_revoke={},
        )

        assert result == "No data objects provided to revoke from share."

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_revoke_data_objects_permission_denied(self, mock_auth, mock_client_class):
        """Test revoking with permission denied."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.shares.update.side_effect = Exception("PERMISSION_DENIED")

        result = revoke_data_object_from_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            objects_to_revoke={"tables": ["catalog.schema.table1"]},
        )

        assert "Permission denied" in result


class TestAddRecipientsToShare:
    """Tests for add_recipients_to_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_recipients_success(self, mock_auth, mock_client_class):
        """Test successful addition of recipient to share."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock share info
        mock_share = MagicMock()
        mock_share.owner = "test_user"
        mock_client.shares.get.return_value = mock_share

        # Mock current user
        mock_user = MagicMock()
        mock_user.user_name = "test_user"
        mock_client.current_user.me.return_value = mock_user

        # Mock recipient info
        mock_recipient = MagicMock()
        mock_recipient.owner = "test_user"
        mock_client.recipients.get.return_value = mock_recipient

        # Mock permissions (recipient doesn't have access yet)
        mock_perms = MagicMock()
        mock_perms.privilege_assignments = []
        mock_client.shares.share_permissions.return_value = mock_perms

        # Mock update permissions
        mock_response = MagicMock()
        mock_client.shares.update_permissions.return_value = mock_response

        result = add_recipients_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            recipient_name="test_recipient",
        )

        assert result is not None
        mock_client.shares.update_permissions.assert_called_once()

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_recipients_not_share_owner(self, mock_auth, mock_client_class):
        """Test adding recipient when not share owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock share info with different owner
        mock_share = MagicMock()
        mock_share.owner = "other_user"
        mock_client.shares.get.return_value = mock_share

        # Mock current user
        mock_user = MagicMock()
        mock_user.user_name = "test_user"
        mock_client.current_user.me.return_value = mock_user

        result = add_recipients_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            recipient_name="test_recipient",
        )

        assert "Permission denied" in result

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_add_recipients_already_has_access(self, mock_auth, mock_client_class):
        """Test adding recipient that already has access."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock share info
        mock_share = MagicMock()
        mock_share.owner = "test_user"
        mock_client.shares.get.return_value = mock_share

        # Mock current user
        mock_user = MagicMock()
        mock_user.user_name = "test_user"
        mock_client.current_user.me.return_value = mock_user

        # Mock recipient info
        mock_recipient = MagicMock()
        mock_recipient.owner = "test_user"
        mock_client.recipients.get.return_value = mock_recipient

        # Mock permissions - recipient already has access
        mock_assignment = MagicMock()
        mock_assignment.principal = "test_recipient"
        mock_assignment.privileges = ["SELECT"]
        mock_perms = MagicMock()
        mock_perms.privilege_assignments = [mock_assignment]
        mock_client.shares.share_permissions.return_value = mock_perms

        result = add_recipients_to_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            recipient_name="test_recipient",
        )

        assert "already has" in result


class TestRemoveRecipientsFromShare:
    """Tests for remove_recipients_from_share function."""

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_remove_recipients_success(self, mock_auth, mock_client_class):
        """Test successful removal of recipient from share."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock share info
        mock_share = MagicMock()
        mock_share.owner = "test_user"
        mock_client.shares.get.return_value = mock_share

        # Mock current user
        mock_user = MagicMock()
        mock_user.user_name = "test_user"
        mock_client.current_user.me.return_value = mock_user

        # Mock recipient info
        mock_recipient = MagicMock()
        mock_recipient.owner = "test_user"
        mock_client.recipients.get.return_value = mock_recipient

        # Mock permissions - recipient has access
        mock_assignment = MagicMock()
        mock_assignment.principal = "test_recipient"
        mock_perms = MagicMock()
        mock_perms.privilege_assignments = [mock_assignment]
        mock_client.shares.share_permissions.return_value = mock_perms

        # Mock update permissions
        mock_response = MagicMock()
        mock_client.shares.update_permissions.return_value = mock_response

        result = remove_recipients_from_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            recipient_name="test_recipient",
        )

        assert result is not None

    @patch("dbrx_api.dltshr.share.WorkspaceClient")
    @patch("dbrx_api.dltshr.share.get_auth_token")
    def test_remove_recipients_no_access(self, mock_auth, mock_client_class):
        """Test removing recipient that doesn't have access."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock share info
        mock_share = MagicMock()
        mock_share.owner = "test_user"
        mock_client.shares.get.return_value = mock_share

        # Mock current user
        mock_user = MagicMock()
        mock_user.user_name = "test_user"
        mock_client.current_user.me.return_value = mock_user

        # Mock recipient info
        mock_recipient = MagicMock()
        mock_recipient.owner = "test_user"
        mock_client.recipients.get.return_value = mock_recipient

        # Mock permissions - recipient doesn't have access
        mock_perms = MagicMock()
        mock_perms.privilege_assignments = []
        mock_client.shares.share_permissions.return_value = mock_perms

        result = remove_recipients_from_share(
            dltshr_workspace_url="https://test.azuredatabricks.net",
            share_name="test_share",
            recipient_name="test_recipient",
        )

        assert "does not have access" in result
