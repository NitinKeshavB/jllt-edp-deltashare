"""Fixtures for Catalog API testing."""


import pytest


@pytest.fixture
def mock_catalog_create_success():
    """Return value for create_catalog_sdk on success."""
    return {
        "success": True,
        "message": "Catalog 'test_catalog' created successfully and privileges granted to service principal",
        "created": True,
    }


@pytest.fixture
def mock_catalog_get_exists():
    """Return value for get_catalog_sdk when catalog exists."""
    return {"exists": True, "owner": "service_principal_id"}


@pytest.fixture
def mock_catalog_get_not_found():
    """Return value for get_catalog_sdk when catalog does not exist."""
    return {"exists": False}


@pytest.fixture
def mock_catalog_list():
    """Return value for list_catalogs_sdk - list of catalog info dicts."""
    return [
        {"name": "catalog1", "owner": "user1"},
        {"name": "catalog2", "owner": "user2"},
    ]


@pytest.fixture
def mock_catalog_list_empty():
    """Return value for list_catalogs_sdk when no catalogs."""
    return []
