"""Test suite for Catalog API endpoints."""

from unittest.mock import patch

from fastapi import status

from tests.consts import API_BASE


class TestCreateCatalogEndpoint:
    """Tests for POST /api/catalogs/{catalog_name} endpoint."""

    def test_create_catalog_success(
        self,
        client,
        mock_catalog_create_success,
    ):
        """Test successfully creating a catalog."""
        with patch("dbrx_api.routes.routes_catalog.create_catalog_sdk") as mock_create:
            mock_create.return_value = mock_catalog_create_success

            response = client.post(
                f"{API_BASE}/catalogs/my_catalog",
                json={"comment": "Test catalog"},
            )

            assert response.status_code == status.HTTP_201_CREATED
            data = response.json()
            assert data["catalog_name"] == "my_catalog"
            assert data["created"] is True
            assert "created successfully" in data["message"]

    def test_create_catalog_already_exists(
        self,
        client,
    ):
        """Test creating catalog when it already exists."""
        with patch("dbrx_api.routes.routes_catalog.create_catalog_sdk") as mock_create:
            mock_create.return_value = {
                "success": False,
                "message": "Catalog 'my_catalog' already exists",
            }

            response = client.post(
                f"{API_BASE}/catalogs/my_catalog",
                json={},
            )

            assert response.status_code == status.HTTP_409_CONFLICT
            assert "already exists" in response.json()["detail"].lower()

    def test_create_catalog_invalid_name_leading_trailing_spaces(
        self,
        client,
    ):
        """Test creating catalog with leading/trailing spaces in name."""
        response = client.post(
            f"{API_BASE}/catalogs/  my_catalog  ",
            json={},
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "cannot have leading or trailing spaces" in response.json()["detail"]

    def test_create_catalog_creation_failure(
        self,
        client,
    ):
        """Test catalog creation failure (e.g. no warehouse)."""
        with patch("dbrx_api.routes.routes_catalog.create_catalog_sdk") as mock_create:
            mock_create.return_value = {
                "success": False,
                "message": "No SQL warehouse available. Please create a SQL warehouse first.",
            }

            response = client.post(
                f"{API_BASE}/catalogs/my_catalog",
                json={},
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "warehouse" in response.json()["detail"].lower()


class TestGetCatalogEndpoint:
    """Tests for GET /api/catalogs/{catalog_name} endpoint."""

    def test_get_catalog_success(
        self,
        client,
        mock_catalog_get_exists,
    ):
        """Test successfully getting catalog details."""
        with patch("dbrx_api.routes.routes_catalog.get_catalog_sdk") as mock_get:
            mock_get.return_value = mock_catalog_get_exists

            response = client.get(f"{API_BASE}/catalogs/my_catalog")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["catalog_name"] == "my_catalog"
            assert data["exists"] is True
            assert "owner" in data

    def test_get_catalog_not_found(
        self,
        client,
        mock_catalog_get_not_found,
    ):
        """Test getting non-existent catalog."""
        with patch("dbrx_api.routes.routes_catalog.get_catalog_sdk") as mock_get:
            mock_get.return_value = mock_catalog_get_not_found

            response = client.get(f"{API_BASE}/catalogs/nonexistent_catalog")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "does not exist" in response.json()["detail"]


class TestListCatalogsEndpoint:
    """Tests for GET /api/catalogs endpoint."""

    def test_list_catalogs_success(
        self,
        client,
        mock_catalog_list,
    ):
        """Test successfully listing catalogs."""
        with patch("dbrx_api.routes.routes_catalog.list_catalogs_sdk") as mock_list:
            mock_list.return_value = mock_catalog_list

            response = client.get(f"{API_BASE}/catalogs")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert "catalogs" in data
            assert data["count"] == 2
            assert len(data["catalogs"]) == 2

    def test_list_catalogs_empty(
        self,
        client,
        mock_catalog_list_empty,
    ):
        """Test listing catalogs when none exist."""
        with patch("dbrx_api.routes.routes_catalog.list_catalogs_sdk") as mock_list:
            mock_list.return_value = mock_catalog_list_empty

            response = client.get(f"{API_BASE}/catalogs")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["catalogs"] == []
            assert data["count"] == 0
