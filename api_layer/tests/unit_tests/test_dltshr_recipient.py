"""Unit tests for dltshr/recipient.py business logic."""

from unittest.mock import MagicMock
from unittest.mock import patch

from databricks.sdk.service.sharing import AuthenticationType
from databricks.sdk.service.sharing import IpAccessList
from databricks.sdk.service.sharing import RecipientInfo

from dbrx_api.dltshr.recipient import add_recipient_ip
from dbrx_api.dltshr.recipient import create_recipient_d2d
from dbrx_api.dltshr.recipient import create_recipient_d2o
from dbrx_api.dltshr.recipient import delete_recipient
from dbrx_api.dltshr.recipient import get_recipients
from dbrx_api.dltshr.recipient import list_recipients
from dbrx_api.dltshr.recipient import revoke_recipient_ip
from dbrx_api.dltshr.recipient import rotate_recipient_token
from dbrx_api.dltshr.recipient import update_recipient_description
from dbrx_api.dltshr.recipient import update_recipient_expiration_time


class TestListRecipients:
    """Tests for list_recipients function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_list_recipients_success(self, mock_auth, mock_client_class):
        """Test successful listing of all recipients."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient1 = MagicMock(spec=RecipientInfo)
        mock_recipient1.name = "recipient1"
        mock_recipient2 = MagicMock(spec=RecipientInfo)
        mock_recipient2.name = "recipient2"
        mock_client.recipients.list.return_value = [mock_recipient1, mock_recipient2]

        result = list_recipients("https://test.azuredatabricks.net")

        assert len(result) == 2

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_list_recipients_with_prefix(self, mock_auth, mock_client_class):
        """Test listing recipients with prefix filter."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient1 = MagicMock(spec=RecipientInfo)
        mock_recipient1.name = "test_recipient1"
        mock_recipient2 = MagicMock(spec=RecipientInfo)
        mock_recipient2.name = "other_recipient"
        mock_client.recipients.list.return_value = [mock_recipient1, mock_recipient2]

        result = list_recipients("https://test.azuredatabricks.net", prefix="test")

        assert len(result) == 1


class TestGetRecipients:
    """Tests for get_recipients function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_get_recipients_success(self, mock_auth, mock_client_class):
        """Test successful retrieval of a recipient."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.name = "test_recipient"
        mock_client.recipients.get.return_value = mock_recipient

        result = get_recipients("test_recipient", "https://test.azuredatabricks.net")

        assert result is not None
        assert result.name == "test_recipient"

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_get_recipients_not_found(self, mock_auth, mock_client_class):
        """Test retrieval when recipient doesn't exist."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.get.side_effect = Exception("Recipient does not exist")

        result = get_recipients("nonexistent", "https://test.azuredatabricks.net")

        assert result is None


class TestCreateRecipientD2D:
    """Tests for create_recipient_d2d function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_create_d2d_success(self, mock_auth, mock_client_class):
        """Test successful D2D recipient creation."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.name = "new_recipient"
        mock_recipient.authentication_type = AuthenticationType.DATABRICKS
        mock_client.recipients.create.return_value = mock_recipient

        result = create_recipient_d2d(
            recipient_name="new_recipient",
            recipient_identifier="cloud:region:uuid",
            description="Test description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result.name == "new_recipient"

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_create_d2d_invalid_identifier(self, mock_auth, mock_client_class):
        """Test D2D creation with invalid identifier."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.create.side_effect = Exception("Cannot resolve target shard")

        result = create_recipient_d2d(
            recipient_name="new_recipient",
            recipient_identifier="invalid",
            description="Test description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "Invalid recipient_identifier" in result

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_create_d2d_already_exists(self, mock_auth, mock_client_class):
        """Test D2D creation when recipient already exists."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.create.side_effect = Exception("There is already a Recipient object")

        result = create_recipient_d2d(
            recipient_name="existing_recipient",
            recipient_identifier="cloud:region:uuid",
            description="Test description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "already exists" in result


class TestCreateRecipientD2O:
    """Tests for create_recipient_d2o function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_create_d2o_success(self, mock_auth, mock_client_class):
        """Test successful D2O recipient creation."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.name = "new_recipient"
        mock_recipient.authentication_type = AuthenticationType.TOKEN
        mock_client.recipients.create.return_value = mock_recipient

        result = create_recipient_d2o(
            recipient_name="new_recipient",
            description="Test description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result.name == "new_recipient"

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_create_d2o_with_ip_list(self, mock_auth, mock_client_class):
        """Test D2O creation with IP access list."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.name = "new_recipient"
        mock_client.recipients.create.return_value = mock_recipient

        result = create_recipient_d2o(
            recipient_name="new_recipient",
            description="Test description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
            ip_access_list=["192.168.1.100", "10.0.0.0/24"],
        )

        assert result is not None
        # Verify IP access list was passed
        call_kwargs = mock_client.recipients.create.call_args.kwargs
        assert call_kwargs.get("ip_access_list") is not None


class TestRotateRecipientToken:
    """Tests for rotate_recipient_token function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_rotate_token_success(self, mock_auth, mock_client_class):
        """Test successful token rotation."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.name = "test_recipient"
        mock_client.recipients.rotate_token.return_value = mock_recipient

        result = rotate_recipient_token(
            recipient_name="test_recipient",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is not None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_rotate_token_cannot_extend(self, mock_auth, mock_client_class):
        """Test token rotation with cannot extend error."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.rotate_token.side_effect = Exception("Cannot extend the token expiration time")

        result = rotate_recipient_token(
            recipient_name="test_recipient",
            dltshr_workspace_url="https://test.azuredatabricks.net",
            expire_in_seconds=999999,
        )

        assert "Cannot extend" in result

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_rotate_token_max_tokens(self, mock_auth, mock_client_class):
        """Test token rotation with max tokens error."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.rotate_token.side_effect = Exception(
            "There are already two activated tokens for recipient"
        )

        result = rotate_recipient_token(
            recipient_name="test_recipient",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "maximum number of active tokens" in result

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_rotate_token_not_owner(self, mock_auth, mock_client_class):
        """Test token rotation when not owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.rotate_token.side_effect = Exception("User is not an owner of Recipient")

        result = rotate_recipient_token(
            recipient_name="test_recipient",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "Permission denied" in result

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_rotate_token_non_token_recipient(self, mock_auth, mock_client_class):
        """Test token rotation on non-TOKEN recipient."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.rotate_token.side_effect = Exception("non-TOKEN authentication type")

        result = rotate_recipient_token(
            recipient_name="test_recipient",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "non-TOKEN type recipient" in result


class TestAddRecipientIP:
    """Tests for add_recipient_ip function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_add_ip_success(self, mock_auth, mock_client_class):
        """Test successful IP addition."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock recipient with existing IPs
        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.ip_access_list = IpAccessList(allowed_ip_addresses=["10.0.0.1"])
        mock_client.recipients.get.return_value = mock_recipient

        mock_updated = MagicMock(spec=RecipientInfo)
        mock_client.recipients.update.return_value = mock_updated

        result = add_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list=["192.168.1.100"],
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is not None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_add_ip_not_owner(self, mock_auth, mock_client_class):
        """Test adding IP when not owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.ip_access_list = None
        mock_client.recipients.get.return_value = mock_recipient
        mock_client.recipients.update.side_effect = Exception("User is not an owner of Recipient")

        result = add_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list=["192.168.1.100"],
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "Permission denied" in result


class TestRevokeRecipientIP:
    """Tests for revoke_recipient_ip function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_revoke_ip_success(self, mock_auth, mock_client_class):
        """Test successful IP revocation."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock recipient with IPs to revoke
        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.ip_access_list = IpAccessList(allowed_ip_addresses=["192.168.1.100", "10.0.0.1"])
        mock_client.recipients.get.return_value = mock_recipient

        mock_updated = MagicMock(spec=RecipientInfo)
        mock_client.recipients.update.return_value = mock_updated

        result = revoke_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list=["192.168.1.100"],
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is not None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_revoke_ip_no_ips(self, mock_auth, mock_client_class):
        """Test revoking IP from recipient with no IPs."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.ip_access_list = None
        mock_client.recipients.get.return_value = mock_recipient

        result = revoke_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list=["192.168.1.100"],
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is None

    def test_revoke_ip_invalid_list(self):
        """Test revoking with invalid list type."""
        result = revoke_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list="not_a_list",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_revoke_ip_empty_list(self, mock_auth, mock_client_class):
        """Test revoking with empty list."""
        mock_auth.return_value = ("test_token", 3600)

        result = revoke_recipient_ip(
            recipient_name="test_recipient",
            ip_access_list=[],
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is None


class TestUpdateRecipientDescription:
    """Tests for update_recipient_description function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_update_description_success(self, mock_auth, mock_client_class):
        """Test successful description update."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_recipient.comment = "New description"
        mock_client.recipients.update.return_value = mock_recipient

        result = update_recipient_description(
            recipient_name="test_recipient",
            description="New description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is not None

    def test_update_description_empty(self):
        """Test updating with empty description."""
        result = update_recipient_description(
            recipient_name="test_recipient",
            description="",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_update_description_not_owner(self, mock_auth, mock_client_class):
        """Test updating description when not owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.update.side_effect = Exception("User is not an owner of Recipient")

        result = update_recipient_description(
            recipient_name="test_recipient",
            description="New description",
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "Permission denied" in result


class TestUpdateRecipientExpirationTime:
    """Tests for update_recipient_expiration_time function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_update_expiration_success(self, mock_auth, mock_client_class):
        """Test successful expiration time update."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_recipient = MagicMock(spec=RecipientInfo)
        mock_client.recipients.update.return_value = mock_recipient

        result = update_recipient_expiration_time(
            recipient_name="test_recipient",
            expiration_time=30,
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert result is not None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_update_expiration_not_owner(self, mock_auth, mock_client_class):
        """Test updating expiration when not owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.update.side_effect = Exception("User is not an owner of Recipient")

        result = update_recipient_expiration_time(
            recipient_name="test_recipient",
            expiration_time=30,
            dltshr_workspace_url="https://test.azuredatabricks.net",
        )

        assert "Permission denied" in result


class TestDeleteRecipient:
    """Tests for delete_recipient function."""

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_delete_recipient_success(self, mock_auth, mock_client_class):
        """Test successful recipient deletion."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.delete.return_value = None

        result = delete_recipient("test_recipient", "https://test.azuredatabricks.net")

        assert result is None

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_delete_recipient_not_owner(self, mock_auth, mock_client_class):
        """Test deletion when not owner."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.delete.side_effect = Exception("User is not an owner of Recipient")

        result = delete_recipient("test_recipient", "https://test.azuredatabricks.net")

        assert "not an owner" in result

    @patch("dbrx_api.dltshr.recipient.WorkspaceClient")
    @patch("dbrx_api.dltshr.recipient.get_auth_token")
    def test_delete_recipient_unauthorized(self, mock_auth, mock_client_class):
        """Test deletion with unauthorized access."""
        mock_auth.return_value = ("test_token", 3600)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.recipients.delete.side_effect = Exception("Unauthorized access")

        result = delete_recipient("test_recipient", "https://test.azuredatabricks.net")

        assert "not an owner" in result

    def test_delete_recipient_empty_name(self):
        """Test deletion with empty name."""
        result = delete_recipient("", "https://test.azuredatabricks.net")

        assert result is None
