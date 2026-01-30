"""Fixtures for logging mocks."""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_logger():
    """Mock loguru logger."""
    with patch("dbrx_api.routes.routes_share.logger") as mock_share_logger, patch(
        "dbrx_api.routes.routes_recipient.logger"
    ) as mock_recipient_logger:
        yield {"share": mock_share_logger, "recipient": mock_recipient_logger}


@pytest.fixture
def mock_azure_blob_handler():
    """Mock Azure Blob Log Handler."""
    with patch("dbrx_api.monitoring.azure_blob_handler.AzureBlobLogHandler") as mock_handler_class:
        mock_handler = MagicMock()
        mock_handler_class.return_value = mock_handler
        yield mock_handler


@pytest.fixture
def mock_postgresql_handler():
    """Mock PostgreSQL Log Handler."""
    with patch("dbrx_api.monitoring.postgresql_handler.PostgreSQLLogHandler") as mock_handler_class:
        mock_handler = MagicMock()
        mock_handler_class.return_value = mock_handler
        yield mock_handler
