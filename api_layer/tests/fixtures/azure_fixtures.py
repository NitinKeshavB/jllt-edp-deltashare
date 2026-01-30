"""Fixtures for Azure service mocks."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_azure_blob_client():
    """Mock Azure Blob Storage client."""
    with patch("azure.storage.blob.BlobServiceClient") as mock_blob_service:
        mock_service_instance = MagicMock()
        mock_blob_client = MagicMock()
        mock_container_client = MagicMock()

        # Setup method chains
        mock_service_instance.get_blob_client.return_value = mock_blob_client
        mock_service_instance.get_container_client.return_value = mock_container_client
        mock_blob_client.upload_blob.return_value = None

        mock_blob_service.return_value = mock_service_instance

        yield mock_service_instance


@pytest.fixture
def mock_postgresql_pool():
    """Mock asyncpg connection pool."""
    mock_pool = AsyncMock()
    mock_connection = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
    mock_connection.execute.return_value = None

    with patch("asyncpg.create_pool", return_value=mock_pool):
        yield mock_pool
