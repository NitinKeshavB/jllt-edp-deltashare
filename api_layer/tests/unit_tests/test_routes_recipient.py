"""Test suite for Recipient API endpoints."""

from datetime import datetime
from datetime import timezone

from databricks.sdk.service.sharing import AuthenticationType
from fastapi import status

from tests.consts import API_BASE


class TestRecipientAuthenticationHeaders:
    """Tests for required authentication headers on Recipient endpoints."""

    def test_missing_workspace_url_header(self, unauthenticated_client):
        """Test that requests without X-Workspace-URL header are rejected."""
        response = unauthenticated_client.get(f"{API_BASE}/recipients")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "X-Workspace-URL" in str(response.json())

    def test_missing_all_headers(self, unauthenticated_client):
        """Test that requests without required headers are rejected."""
        response = unauthenticated_client.get(f"{API_BASE}/recipients")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "X-Workspace-URL" in str(response.json())


class TestGetRecipient:
    """Tests for GET /recipients/{recipient_name} endpoint."""

    def test_get_recipient_by_name_success(self, client, mock_recipient_business_logic):
        """Test successful retrieval of a recipient by name."""
        response = client.get(f"{API_BASE}/recipients/test_recipient")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == "test_recipient"
        assert data["owner"] == "test_owner"
        mock_recipient_business_logic["get"].assert_called_once()

    def test_get_recipient_by_name_not_found(self, client, mock_recipient_business_logic):
        """Test retrieval of non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.get(f"{API_BASE}/recipients/nonexistent_recipient")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()


class TestListRecipients:
    """Tests for GET /recipients endpoint."""

    def test_list_all_recipients_success(self, client, mock_recipient_business_logic):
        """Test successful listing of all recipients."""
        response = client.get(f"{API_BASE}/recipients")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "Message" in data
        assert "Recipient" in data
        assert len(data["Recipient"]) == 2
        assert "Fetched 2 recipients!" in data["Message"]

    def test_list_recipients_with_prefix(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test listing recipients with prefix filter."""
        mock_recipient_business_logic["list"].return_value = [mock_recipient_info(name="test_recipient1")]

        response = client.get(f"{API_BASE}/recipients?prefix=test")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["Recipient"]) == 1

    def test_list_recipients_with_page_size(self, client, mock_recipient_business_logic):
        """Test listing recipients with custom page size."""
        response = client.get(f"{API_BASE}/recipients?page_size=50")

        assert response.status_code == status.HTTP_200_OK
        mock_recipient_business_logic["list"].assert_called_once()

    def test_list_recipients_empty_result(self, client, mock_recipient_business_logic):
        """Test listing recipients when no recipients exist."""
        mock_recipient_business_logic["list"].return_value = []

        response = client.get(f"{API_BASE}/recipients")

        assert response.status_code == status.HTTP_200_OK
        assert "No recipients found" in response.json()["detail"]

    def test_list_recipients_invalid_page_size(self, client):
        """Test listing recipients with invalid page size."""
        response = client.get(f"{API_BASE}/recipients?page_size=0")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestDeleteRecipient:
    """Tests for DELETE /recipients/{recipient_name} endpoint."""

    def test_delete_recipient_success(self, client, mock_recipient_business_logic):
        """Test successful deletion of a recipient."""
        response = client.delete(f"{API_BASE}/recipients/test_recipient")

        assert response.status_code == status.HTTP_200_OK
        assert "Deleted Recipient successfully" in response.json()["message"]
        mock_recipient_business_logic["delete"].assert_called_once()

    def test_delete_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test deletion of non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.delete(f"{API_BASE}/recipients/nonexistent_recipient")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()

    def test_delete_recipient_permission_denied(self, client, mock_recipient_business_logic):
        """Test deletion when user is not the owner."""
        mock_recipient_business_logic["delete"].return_value = "User is not an owner of Recipient"

        response = client.delete(f"{API_BASE}/recipients/test_recipient")

        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "Permission denied" in response.json()["detail"]


class TestCreateRecipientD2D:
    """Tests for POST /recipients/d2d/{recipient_name} endpoint."""

    def test_create_d2d_recipient_success(self, client, mock_recipient_business_logic):
        """Test successful creation of a Databricks-to-Databricks recipient."""
        # Mock get to return None (recipient doesn't exist)
        mock_recipient_business_logic["get"].return_value = None

        response = client.post(
            f"{API_BASE}/recipients/d2d/new_d2d_recipient",
            params={
                "recipient_identifier": "metastore-id-12345",
                "description": "D2D recipient for testing",
            },
        )

        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == "new_d2d_recipient"
        assert data["authentication_type"] == "DATABRICKS"
        mock_recipient_business_logic["create_d2d"].assert_called_once()

    def test_create_d2d_recipient_already_exists(self, client, mock_recipient_business_logic):
        """Test creation of a D2D recipient that already exists."""
        response = client.post(
            f"{API_BASE}/recipients/d2d/test_recipient",
            params={
                "recipient_identifier": "metastore-id-12345",
                "description": "Duplicate recipient",
            },
        )

        assert response.status_code == status.HTTP_409_CONFLICT
        assert "already exists" in response.json()["detail"].lower()

    def test_create_d2d_recipient_invalid_identifier(self, client, mock_recipient_business_logic):
        """Test creation with invalid recipient identifier."""
        mock_recipient_business_logic["get"].return_value = None
        mock_recipient_business_logic["create_d2d"].return_value = "Invalid recipient_identifier format: invalid-id"

        response = client.post(
            f"{API_BASE}/recipients/d2d/new_recipient",
            params={
                "recipient_identifier": "invalid-id",
                "description": "Test recipient",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid recipient_identifier" in response.json()["detail"]


class TestCreateRecipientD2O:
    """Tests for POST /recipients/d2o/{recipient_name} endpoint."""

    def test_create_d2o_recipient_success(self, client, mock_recipient_business_logic):
        """Test successful creation of a Databricks-to-Open recipient."""
        # Mock get to return None (recipient doesn't exist)
        mock_recipient_business_logic["get"].return_value = None

        response = client.post(
            f"{API_BASE}/recipients/d2o/new_d2o_recipient",
            params={
                "description": "D2O recipient for testing",
            },
        )

        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == "new_d2o_recipient"
        assert data["authentication_type"] == "TOKEN"
        mock_recipient_business_logic["create_d2o"].assert_called_once()

    def test_create_d2o_recipient_already_exists(self, client, mock_recipient_business_logic):
        """Test creation of a D2O recipient that already exists."""
        response = client.post(
            f"{API_BASE}/recipients/d2o/test_recipient",
            params={
                "description": "Duplicate recipient",
            },
        )

        assert response.status_code == status.HTTP_409_CONFLICT
        assert "already exists" in response.json()["detail"].lower()

    def test_create_d2o_recipient_with_expiration(self, client, mock_recipient_business_logic):
        """Test creation with token expiration time."""
        mock_recipient_business_logic["get"].return_value = None

        expiration_time = int(datetime.now(timezone.utc).timestamp() * 1000) + 86400000  # 24 hours

        response = client.post(
            f"{API_BASE}/recipients/d2o/new_recipient",
            params={
                "description": "Test recipient",
                "token_expiration_time_ms": expiration_time,
            },
        )

        assert response.status_code == status.HTTP_201_CREATED

    def test_create_d2o_recipient_with_valid_ips(self, client, mock_recipient_business_logic):
        """Test creation with valid IP access list."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.post(
            f"{API_BASE}/recipients/d2o/new_recipient",
            params={
                "description": "Test recipient",
                "ip_access_list": ["192.168.1.100", "10.0.0.0/24"],
            },
        )

        assert response.status_code == status.HTTP_201_CREATED

    def test_create_d2o_recipient_with_invalid_ips(self, client, mock_recipient_business_logic):
        """Test creation with invalid IP addresses."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.post(
            f"{API_BASE}/recipients/d2o/new_recipient",
            params={
                "description": "Test recipient",
                "ip_access_list": ["invalid-ip", "999.999.999.999"],
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid IP addresses or CIDR blocks" in response.json()["detail"]


class TestRotateRecipientToken:
    """Tests for PUT /recipients/{recipient_name}/tokens/rotate endpoint."""

    def test_rotate_token_success(self, client, mock_recipient_business_logic):
        """Test successful token rotation."""
        response = client.put(f"{API_BASE}/recipients/test_recipient/tokens/rotate")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "tokens" in data
        assert len(data["tokens"]) > 0
        mock_recipient_business_logic["rotate"].assert_called_once()

    def test_rotate_token_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test token rotation for non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.put(f"{API_BASE}/recipients/nonexistent_recipient/tokens/rotate")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()

    def test_rotate_token_d2d_recipient(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test token rotation for D2D recipient (currently allowed by implementation)."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="d2d_recipient", auth_type=AuthenticationType.DATABRICKS
        )

        response = client.put(f"{API_BASE}/recipients/d2d_recipient/tokens/rotate")

        # Current implementation allows token rotation for all recipient types
        assert response.status_code == status.HTTP_200_OK

    def test_rotate_token_with_expiration(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test token rotation with custom expiration time."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        expire_in_seconds = 86400  # 1 day in seconds

        response = client.put(
            f"{API_BASE}/recipients/test_recipient/tokens/rotate?expire_in_seconds={expire_in_seconds}"
        )

        assert response.status_code == status.HTTP_200_OK


class TestAddClientIPToRecipient:
    """Tests for PUT /recipients/{recipient_name}/ipaddress/add endpoint."""

    def test_add_ip_success(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test successful addition of IP address."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        response = client.put(f"{API_BASE}/recipients/test_recipient/ipaddress/add?ip_access_list=192.168.1.100")

        assert response.status_code == status.HTTP_200_OK
        mock_recipient_business_logic["add_ip"].assert_called_once()

    def test_add_ip_with_cidr(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test addition of IP address with CIDR notation."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        response = client.put(f"{API_BASE}/recipients/test_recipient/ipaddress/add?ip_access_list=192.168.1.0/24")

        assert response.status_code == status.HTTP_200_OK

    def test_add_ip_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test adding IP to non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.put(
            f"{API_BASE}/recipients/nonexistent_recipient/ipaddress/add?ip_access_list=192.168.1.100"
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_add_ip_to_d2d_recipient(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test adding IP to D2D recipient (should fail)."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="d2d_recipient", auth_type=AuthenticationType.DATABRICKS
        )

        response = client.put(f"{API_BASE}/recipients/d2d_recipient/ipaddress/add?ip_access_list=192.168.1.100")

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "DATABRICKS to DATABRICKS" in response.json()["detail"]

    def test_add_ip_invalid_format(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test adding invalid IP address."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        response = client.put(f"{API_BASE}/recipients/test_recipient/ipaddress/add?ip_access_list=invalid-ip")

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid IP address" in response.json()["detail"]


class TestRevokeClientIPFromRecipient:
    """Tests for PUT /recipients/{recipient_name}/ipaddress/revoke endpoint."""

    def test_revoke_ip_success(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test successful revocation of IP address."""
        from databricks.sdk.service.sharing import IpAccessList

        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient",
            auth_type=AuthenticationType.TOKEN,
            ip_access_list=IpAccessList(allowed_ip_addresses=["192.168.1.100"]),
        )

        response = client.put(f"{API_BASE}/recipients/test_recipient/ipaddress/revoke?ip_access_list=192.168.1.100")

        assert response.status_code == status.HTTP_200_OK
        mock_recipient_business_logic["revoke_ip"].assert_called_once()

    def test_revoke_ip_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test revoking IP from non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.put(
            f"{API_BASE}/recipients/nonexistent_recipient/ipaddress/revoke?ip_access_list=192.168.1.100"
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_revoke_ip_from_d2d_recipient(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test revoking IP from D2D recipient (should fail)."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="d2d_recipient", auth_type=AuthenticationType.DATABRICKS
        )

        response = client.put(f"{API_BASE}/recipients/d2d_recipient/ipaddress/revoke?ip_access_list=192.168.1.100")

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "DATABRICKS to DATABRICKS" in response.json()["detail"]

    def test_revoke_ip_invalid_format(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test revoking invalid IP address."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        response = client.put(f"{API_BASE}/recipients/test_recipient/ipaddress/revoke?ip_access_list=invalid-ip")

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid IP address" in response.json()["detail"]


class TestUpdateRecipientDescription:
    """Tests for PUT /recipients/{recipient_name}/description/update endpoint."""

    def test_update_description_success(self, client, mock_recipient_business_logic):
        """Test successful update of recipient description."""
        response = client.put(
            f"{API_BASE}/recipients/test_recipient/description/update", params={"description": "Updated description"}
        )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["comment"] == "Updated description"
        mock_recipient_business_logic["update_desc"].assert_called_once()

    def test_update_description_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test updating description for non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        response = client.put(
            f"{API_BASE}/recipients/nonexistent_recipient/description/update",
            params={"description": "Updated description"},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_description_permission_denied(self, client, mock_recipient_business_logic):
        """Test updating description without permission."""
        mock_recipient_business_logic["update_desc"].return_value = "User is not an owner of Recipient"

        response = client.put(
            f"{API_BASE}/recipients/test_recipient/description/update", params={"description": "Updated description"}
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN


class TestUpdateRecipientExpiration:
    """Tests for PUT /recipients/{recipient_name}/expiration_time/update endpoint."""

    def test_update_expiration_success(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test successful update of token expiration time."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )

        expiration_time_in_days = 1  # 1 day

        response = client.put(
            f"{API_BASE}/recipients/test_recipient/expiration_time/update",
            params={"expiration_time_in_days": expiration_time_in_days},
        )

        assert response.status_code == status.HTTP_200_OK
        mock_recipient_business_logic["update_exp"].assert_called_once()

    def test_update_expiration_recipient_not_found(self, client, mock_recipient_business_logic):
        """Test updating expiration for non-existent recipient."""
        mock_recipient_business_logic["get"].return_value = None

        expiration_time_in_days = 1  # 1 day

        response = client.put(
            f"{API_BASE}/recipients/nonexistent_recipient/expiration_time/update",
            params={"expiration_time_in_days": expiration_time_in_days},
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_expiration_d2d_recipient(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test updating expiration for D2D recipient (should fail)."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="d2d_recipient", auth_type=AuthenticationType.DATABRICKS
        )

        expiration_time_in_days = 1  # 1 day

        response = client.put(
            f"{API_BASE}/recipients/d2d_recipient/expiration_time/update",
            params={"expiration_time_in_days": expiration_time_in_days},
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "DATABRICKS" in response.json()["detail"]

    def test_update_expiration_permission_denied(self, client, mock_recipient_business_logic, mock_recipient_info):
        """Test updating expiration without permission."""
        mock_recipient_business_logic["get"].return_value = mock_recipient_info(
            name="test_recipient", auth_type=AuthenticationType.TOKEN
        )
        mock_recipient_business_logic["update_exp"].return_value = "User is not an owner of Recipient"

        expiration_time_in_days = 1  # 1 day

        response = client.put(
            f"{API_BASE}/recipients/test_recipient/expiration_time/update",
            params={"expiration_time_in_days": expiration_time_in_days},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN
