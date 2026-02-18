"""Test suite for Share API endpoints."""

from databricks.sdk.service.sharing import UpdateSharePermissionsResponse
from fastapi import status

from tests.consts import API_BASE


class TestShareAuthenticationHeaders:
    """Tests for required authentication headers on Share endpoints."""

    def test_missing_workspace_url_header(self, unauthenticated_client):
        """Test that requests without X-Workspace-URL header are rejected."""
        response = unauthenticated_client.get(f"{API_BASE}/shares")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "X-Workspace-URL" in str(response.json())

    def test_missing_all_headers(self, unauthenticated_client):
        """Test that requests without required headers are rejected."""
        response = unauthenticated_client.get(f"{API_BASE}/shares")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "X-Workspace-URL" in str(response.json())


class TestWorkspaceUrlValidation:
    """Tests for Databricks workspace URL format validation."""

    def test_valid_azure_workspace_url(self, unauthenticated_client):
        """Test that Azure Databricks URL pattern is accepted."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://adb-1234567890123456.12.azuredatabricks.net",
            },
        )
        # Should not fail on URL validation (may fail later on business logic)
        assert response.status_code != status.HTTP_400_BAD_REQUEST

    def test_valid_aws_workspace_url(self, unauthenticated_client):
        """Test that AWS Databricks URL pattern is accepted."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://my-workspace.cloud.databricks.com",
            },
        )
        # Should not fail on URL validation
        assert response.status_code != status.HTTP_400_BAD_REQUEST

    def test_valid_gcp_workspace_url(self, unauthenticated_client):
        """Test that GCP Databricks URL pattern is accepted."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://my-workspace.gcp.databricks.com",
            },
        )
        # Should not fail on URL validation
        assert response.status_code != status.HTTP_400_BAD_REQUEST

    def test_invalid_non_databricks_url(self, unauthenticated_client):
        """Test that non-Databricks URLs are rejected."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://example.com",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid Databricks workspace URL format" in response.json()["detail"]

    def test_invalid_http_url(self, unauthenticated_client):
        """Test that HTTP (non-HTTPS) URLs are rejected."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "http://test.azuredatabricks.net",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "HTTPS" in response.json()["detail"]

    def test_invalid_random_domain_url(self, unauthenticated_client):
        """Test that random domain URLs are rejected."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://malicious-site.io/databricks",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid Databricks workspace URL format" in response.json()["detail"]

    def test_invalid_partial_databricks_url(self, unauthenticated_client):
        """Test that partial Databricks-like URLs are rejected."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://fake-azuredatabricks.net.evil.com",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_url_with_trailing_slash_accepted(self, unauthenticated_client):
        """Test that URLs with trailing slash are accepted and normalized."""
        response = unauthenticated_client.get(
            f"{API_BASE}/shares",
            headers={
                "X-Workspace-URL": "https://test-workspace.azuredatabricks.net/",
            },
        )
        # Should not fail on URL validation
        assert response.status_code != status.HTTP_400_BAD_REQUEST

    def test_unreachable_workspace_returns_502(self, unauthenticated_client):
        """Test that unreachable workspace URLs return 502 with clear error message."""
        from unittest.mock import AsyncMock
        from unittest.mock import patch

        # Mock the reachability check to return unreachable
        with patch("dbrx_api.dependencies.check_workspace_reachable", new_callable=AsyncMock) as mock_check:
            mock_check.return_value = (False, "Workspace hostname 'fake.azuredatabricks.net' could not be resolved.")

            response = unauthenticated_client.get(
                f"{API_BASE}/shares",
                headers={
                    "X-Workspace-URL": "https://fake.azuredatabricks.net",
                },
            )

            assert response.status_code == status.HTTP_502_BAD_GATEWAY
            assert "could not be resolved" in response.json()["detail"]

    def test_workspace_timeout_returns_502(self, unauthenticated_client):
        """Test that workspace connection timeout returns 502."""
        from unittest.mock import AsyncMock
        from unittest.mock import patch

        with patch("dbrx_api.dependencies.check_workspace_reachable", new_callable=AsyncMock) as mock_check:
            mock_check.return_value = (False, "Connection to workspace timed out.")

            response = unauthenticated_client.get(
                f"{API_BASE}/shares",
                headers={
                    "X-Workspace-URL": "https://slow-workspace.azuredatabricks.net",
                },
            )

            assert response.status_code == status.HTTP_502_BAD_GATEWAY
            assert "timed out" in response.json()["detail"]


class TestGetShareByName:
    """Tests for GET /shares/{share_name} endpoint."""

    def test_get_share_by_name_success(self, client, mock_share_business_logic):
        """Test successful retrieval of a share by name."""
        response = client.get(f"{API_BASE}/shares/test_share")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == "test_share"
        assert data["owner"] == "test_owner"
        mock_share_business_logic["get"].assert_called_once()

    def test_get_share_by_name_not_found(self, client, mock_share_business_logic):
        """Test retrieval of non-existent share."""
        mock_share_business_logic["get"].return_value = None

        response = client.get(f"{API_BASE}/shares/nonexistent_share")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()


class TestListShares:
    """Tests for GET /shares endpoint."""

    def test_list_all_shares_success(self, client, mock_share_business_logic):
        """Test successful listing of all shares."""
        response = client.get(f"{API_BASE}/shares")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "Message" in data
        assert "Share" in data
        assert len(data["Share"]) == 2
        assert "Fetched 2 shares!" in data["Message"]

    def test_list_shares_with_prefix(self, client, mock_share_business_logic, mock_share_info):
        """Test listing shares with prefix filter."""
        mock_share_business_logic["list"].return_value = [mock_share_info(name="test_share1")]

        response = client.get(f"{API_BASE}/shares?prefix=test")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["Share"]) == 1

    def test_list_shares_with_page_size(self, client, mock_share_business_logic):
        """Test listing shares with custom page size."""
        response = client.get(f"{API_BASE}/shares?page_size=50")

        assert response.status_code == status.HTTP_200_OK
        mock_share_business_logic["list"].assert_called_once()

    def test_list_shares_empty_result(self, client, mock_share_business_logic):
        """Test listing shares when no shares exist."""
        mock_share_business_logic["list"].return_value = []

        response = client.get(f"{API_BASE}/shares")

        assert response.status_code == status.HTTP_204_NO_CONTENT
        assert "No shares found" in response.json()["detail"]

    def test_list_shares_invalid_page_size(self, client):
        """Test listing shares with invalid page size."""
        response = client.get(f"{API_BASE}/shares?page_size=0")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestDeleteShare:
    """Tests for DELETE /shares/{share_name} endpoint."""

    def test_delete_share_success(self, client, mock_share_business_logic):
        """Test successful deletion of a share."""
        response = client.delete(f"{API_BASE}/shares/test_share")

        assert response.status_code == status.HTTP_200_OK
        assert "Deleted Share successfully" in response.json()["message"]

    def test_delete_share_not_found(self, client, mock_share_business_logic):
        """Test deletion of non-existent share."""
        mock_share_business_logic["get"].return_value = None

        response = client.delete(f"{API_BASE}/shares/nonexistent_share")

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()

    def test_delete_share_permission_denied(self, client, mock_share_business_logic):
        """Test deletion when user is not the owner."""
        mock_share_business_logic["delete"].return_value = "User is not an owner of Share"

        response = client.delete(f"{API_BASE}/shares/test_share")

        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "Permission denied" in response.json()["detail"]

    def test_delete_share_not_found_during_deletion(self, client, mock_share_business_logic):
        """Test when share is deleted between check and deletion."""
        mock_share_business_logic["delete"].return_value = "Share not found"

        response = client.delete(f"{API_BASE}/shares/test_share")

        assert response.status_code == status.HTTP_404_NOT_FOUND


class TestCreateShare:
    """Tests for POST /shares/{share_name} endpoint."""

    def test_create_share_success(self, client, mock_share_business_logic):
        """Test successful creation of a share."""
        # Mock get to return None (share doesn't exist)
        mock_share_business_logic["get"].return_value = None

        response = client.post(
            f"{API_BASE}/shares/new_share",
            params={"description": "Test share description", "storage_root": "s3://test-bucket/"},
        )

        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["name"] == "new_share"
        mock_share_business_logic["create"].assert_called_once()

    def test_create_share_already_exists(self, client, mock_share_business_logic):
        """Test creation of a share that already exists."""
        response = client.post(f"{API_BASE}/shares/test_share", params={"description": "Duplicate share"})

        assert response.status_code == status.HTTP_409_CONFLICT
        assert "already exists" in response.json()["detail"].lower()

    def test_create_share_empty_name(self, client):
        """Test creation with empty share name."""
        response = client.post(f"{API_BASE}/shares/ ", params={"description": "Test description"})

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "cannot be empty" in response.json()["detail"]

    def test_create_share_invalid_name_format(self, client):
        """Test creation with invalid share name format."""
        response = client.post(f"{API_BASE}/shares/invalid.share", params={"description": "Test description"})

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid share name" in response.json()["detail"]

    def test_create_share_invalid_name_with_special_chars(self, client):
        """Test creation with special characters in share name."""
        invalid_names = ["share/name", "share name", "share.name", "share@name"]

        for invalid_name in invalid_names:
            response = client.post(f"{API_BASE}/shares/{invalid_name}", params={"description": "Test description"})
            assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_create_share_sdk_error(self, client, mock_share_business_logic):
        """Test creation when SDK returns error."""
        mock_share_business_logic["get"].return_value = None
        mock_share_business_logic["create"].return_value = "is not a valid name"

        response = client.post(f"{API_BASE}/shares/invalid_name", params={"description": "Test description"})

        assert response.status_code == status.HTTP_400_BAD_REQUEST


class TestAddDataObjectsToShare:
    """Tests for PUT /shares/{share_name}/dataobject/add endpoint."""

    def test_add_data_objects_success(self, client, mock_share_business_logic):
        """Test successful addition of data objects to share."""
        payload = {
            "tables": ["catalog.schema.table1", "catalog.schema.table2"],
            "views": ["catalog.schema.view1"],
            "schemas": ["catalog.schema"],
        }

        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_200_OK
        mock_share_business_logic["add_objects"].assert_called_once()

    def test_add_data_objects_share_not_found(self, client, mock_share_business_logic):
        """Test adding data objects to non-existent share."""
        mock_share_business_logic["get"].return_value = None

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/nonexistent_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()

    def test_add_data_objects_already_exists(self, client, mock_share_business_logic):
        """Test adding data object that already exists in share."""
        mock_share_business_logic["add_objects"].return_value = "Data object already exists"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_409_CONFLICT

    def test_add_data_objects_permission_denied(self, client, mock_share_business_logic):
        """Test adding data objects without permission."""
        mock_share_business_logic["add_objects"].return_value = "Permission denied"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_add_data_objects_not_found_error(self, client, mock_share_business_logic):
        """Test adding non-existent data objects."""
        mock_share_business_logic["add_objects"].return_value = "Table not found"

        payload = {"tables": ["catalog.schema.nonexistent_table"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_add_data_objects_no_objects_provided(self, client, mock_share_business_logic):
        """Test adding data objects without providing any objects."""
        mock_share_business_logic["add_objects"].return_value = "No data objects provided"

        payload = {"tables": [], "views": [], "schemas": []}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_add_data_objects_invalid_parameter(self, client, mock_share_business_logic):
        """Test adding data objects with invalid parameters."""
        mock_share_business_logic["add_objects"].return_value = "Invalid parameter"

        payload = {"tables": ["invalid_table_name"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_add_data_objects_generic_error(self, client, mock_share_business_logic):
        """Test adding data objects with a generic error."""
        mock_share_business_logic["add_objects"].return_value = "Some unexpected error occurred"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/add", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Some unexpected error" in response.json()["detail"]


class TestRevokeDataObjectsFromShare:
    """Tests for PUT /shares/{share_name}/dataobject/revoke endpoint."""

    def test_revoke_data_objects_success(self, client, mock_share_business_logic):
        """Test successful revocation of data objects from share."""
        payload = {
            "tables": ["catalog.schema.table1"],
            "views": ["catalog.schema.view1"],
        }

        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_200_OK
        mock_share_business_logic["revoke_objects"].assert_called_once()

    def test_revoke_data_objects_share_not_found(self, client, mock_share_business_logic):
        """Test revoking data objects from non-existent share."""
        mock_share_business_logic["get"].return_value = None

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/nonexistent_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_revoke_data_objects_permission_denied(self, client, mock_share_business_logic):
        """Test revoking data objects without permission."""
        mock_share_business_logic["revoke_objects"].return_value = "User is not an owner"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_revoke_data_objects_not_found(self, client, mock_share_business_logic):
        """Test revoking non-existent data objects."""
        mock_share_business_logic["revoke_objects"].return_value = "Data object not found"

        payload = {"tables": ["catalog.schema.nonexistent_table"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_revoke_data_objects_no_objects_provided(self, client, mock_share_business_logic):
        """Test revoking without providing any objects."""
        mock_share_business_logic["revoke_objects"].return_value = "No data objects provided"

        payload = {}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_revoke_data_objects_cannot_remove_schemas(self, client, mock_share_business_logic):
        """Test revoking with 'Cannot remove schemas' error."""
        mock_share_business_logic["revoke_objects"].return_value = "Cannot remove schemas from share"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Cannot remove schemas" in response.json()["detail"]

    def test_revoke_data_objects_generic_error(self, client, mock_share_business_logic):
        """Test revoking with a generic error."""
        mock_share_business_logic["revoke_objects"].return_value = "Some unexpected revocation error"

        payload = {"tables": ["catalog.schema.table1"]}
        response = client.put(f"{API_BASE}/shares/test_share/dataobject/revoke", json=payload)

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Some unexpected revocation error" in response.json()["detail"]


class TestAddRecipientToShare:
    """Tests for PUT /shares/{share_name}/recipients/add endpoint."""

    def test_add_recipient_success(self, client, mock_share_business_logic):
        """Test successful addition of recipient to share."""
        mock_share_business_logic["add_recipients"].return_value = UpdateSharePermissionsResponse()

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/add", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_200_OK
        mock_share_business_logic["add_recipients"].assert_called_once()

    def test_add_recipient_already_has_access(self, client, mock_share_business_logic):
        """Test adding recipient that already has access."""
        mock_share_business_logic["add_recipients"].return_value = "Recipient already has access"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/add", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_409_CONFLICT

    def test_add_recipient_permission_denied(self, client, mock_share_business_logic):
        """Test adding recipient without permission."""
        mock_share_business_logic["add_recipients"].return_value = "Permission denied - not an owner"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/add", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_add_recipient_share_not_found(self, client, mock_share_business_logic):
        """Test adding recipient to non-existent share."""
        mock_share_business_logic["add_recipients"].return_value = "Share not found"

        response = client.put(
            f"{API_BASE}/shares/nonexistent_share/recipients/add", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_add_recipient_recipient_not_found(self, client, mock_share_business_logic):
        """Test adding non-existent recipient."""
        mock_share_business_logic["add_recipients"].return_value = "Recipient does not exist"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/add", params={"recipient_name": "nonexistent_recipient"}
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_add_recipient_generic_error(self, client, mock_share_business_logic):
        """Test adding recipient with a generic error."""
        mock_share_business_logic["add_recipients"].return_value = "Some unexpected add recipient error"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/add", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Some unexpected add recipient error" in response.json()["detail"]


class TestRemoveRecipientFromShare:
    """Tests for PUT /shares/{share_name}/recipients/remove endpoint."""

    def test_remove_recipient_success(self, client, mock_share_business_logic):
        """Test successful removal of recipient from share."""
        mock_share_business_logic["remove_recipients"].return_value = UpdateSharePermissionsResponse()

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/remove", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_200_OK
        mock_share_business_logic["remove_recipients"].assert_called_once()

    def test_remove_recipient_permission_denied(self, client, mock_share_business_logic):
        """Test removing recipient without permission."""
        mock_share_business_logic["remove_recipients"].return_value = "Permission denied - not an owner"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/remove", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_remove_recipient_share_not_found(self, client, mock_share_business_logic):
        """Test removing recipient from non-existent share."""
        mock_share_business_logic["remove_recipients"].return_value = "Share not found"

        response = client.put(
            f"{API_BASE}/shares/nonexistent_share/recipients/remove", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_remove_recipient_does_not_have_access(self, client, mock_share_business_logic):
        """Test removing recipient that doesn't have access."""
        mock_share_business_logic["remove_recipients"].return_value = "Recipient does not have access to share"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/remove", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_remove_recipient_generic_error(self, client, mock_share_business_logic):
        """Test removing recipient with a generic error."""
        mock_share_business_logic["remove_recipients"].return_value = "Some unexpected remove recipient error"

        response = client.put(
            f"{API_BASE}/shares/test_share/recipients/remove", params={"recipient_name": "test_recipient"}
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Some unexpected remove recipient error" in response.json()["detail"]


class TestDatabricksErrorHandling:
    """Tests for Databricks SDK error handling."""

    def test_unauthenticated_error_returns_401(self, app, client):
        """Test that Databricks Unauthenticated errors return 401."""
        from unittest.mock import patch

        from databricks.sdk.errors import Unauthenticated

        # Mock the business logic to raise Unauthenticated
        with patch("dbrx_api.routes.routes_share.get_shares") as mock_get:
            mock_get.side_effect = Unauthenticated("Invalid token")

            response = client.get(f"{API_BASE}/shares/test_share")

            assert response.status_code == status.HTTP_401_UNAUTHORIZED
            assert "authentication failed" in response.json()["detail"].lower()

    def test_permission_denied_error_returns_403(self, app, client):
        """Test that Databricks PermissionDenied errors return 403."""
        from unittest.mock import patch

        from databricks.sdk.errors import PermissionDenied

        with patch("dbrx_api.routes.routes_share.get_shares") as mock_get:
            mock_get.side_effect = PermissionDenied("User not authorized")

            response = client.get(f"{API_BASE}/shares/test_share")

            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "access denied" in response.json()["detail"].lower()

    def test_not_found_error_returns_404(self, app, client):
        """Test that Databricks NotFound errors return 404."""
        from unittest.mock import patch

        from databricks.sdk.errors import NotFound

        with patch("dbrx_api.routes.routes_share.get_shares") as mock_get:
            mock_get.side_effect = NotFound("Resource not found")

            response = client.get(f"{API_BASE}/shares/test_share")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_bad_request_error_returns_400(self, app, client):
        """Test that Databricks BadRequest errors return 400."""
        from unittest.mock import patch

        from databricks.sdk.errors import BadRequest

        with patch("dbrx_api.routes.routes_share.get_shares") as mock_get:
            mock_get.side_effect = BadRequest("Invalid parameter")

            response = client.get(f"{API_BASE}/shares/test_share")

            assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_generic_databricks_error_returns_502(self, app, client):
        """Test that generic Databricks errors return 502 Bad Gateway."""
        from unittest.mock import patch

        from databricks.sdk.errors import DatabricksError

        with patch("dbrx_api.routes.routes_share.get_shares") as mock_get:
            mock_get.side_effect = DatabricksError("Some internal error")

            response = client.get(f"{API_BASE}/shares/test_share")

            assert response.status_code == status.HTTP_502_BAD_GATEWAY
            assert "databricks service error" in response.json()["detail"].lower()
