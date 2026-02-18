"""Fixtures for Workflow API testing."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture
def workflow_settings():
    """Settings with workflow enabled for testing workflow routes."""
    with patch.dict(
        "os.environ",
        {
            "DLTSHR_WORKSPACE_URL": "https://test-workspace.azuredatabricks.net/",
            "CLIENT_ID": "test-client-id",
            "CLIENT_SECRET": "test-client-secret",
            "ACCOUNT_ID": "test-account-id",
            "ENABLE_WORKFLOW": "true",
            "DOMAIN_DB_CONNECTION_STRING": "postgresql://localhost/workflow_test",
            "AZURE_QUEUE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            "AZURE_QUEUE_NAME": "sharepack-test",
            "ENABLE_BLOB_LOGGING": "false",
            "ENABLE_POSTGRESQL_LOGGING": "false",
        },
    ):
        from dbrx_api.settings import Settings

        yield Settings()


@pytest.fixture
def mock_domain_db_pool():
    """Mock DomainDBPool for workflow tests."""
    pool = MagicMock()
    pool.initialize = AsyncMock(return_value=None)
    pool.close = AsyncMock(return_value=None)
    pool.health_check = AsyncMock(return_value=True)
    pool.get_table_counts = AsyncMock(return_value={"share_packs": 0})
    return pool


@pytest.fixture
def mock_queue_client():
    """Mock SharePackQueueClient for workflow tests."""
    client = MagicMock()
    client.get_queue_length = MagicMock(return_value=0)
    client.send_message = MagicMock(return_value=None)
    client.enqueue_sharepack = MagicMock(return_value=None)
    return client


@pytest.fixture
def app_with_workflow(workflow_settings, mock_domain_db_pool, mock_queue_client):
    """FastAPI app with workflow router enabled and mocked DB/queue."""

    async def noop_start_queue_consumer(*args, **kwargs):
        """No-op so startup does not run a real consumer loop."""
        return

    with (
        patch("dbrx_api.workflow.db.pool.DomainDBPool", MagicMock(return_value=mock_domain_db_pool)),
        patch("dbrx_api.workflow.queue.queue_client.SharePackQueueClient", MagicMock(return_value=mock_queue_client)),
        patch("dbrx_api.workflow.queue.queue_consumer.start_queue_consumer", side_effect=noop_start_queue_consumer),
    ):
        from dbrx_api.main import create_app

        app = create_app(settings=workflow_settings)
        yield app


@pytest.fixture
def client_with_workflow(app_with_workflow):
    """Test client using app with workflow enabled (with auth headers)."""
    from tests.fixtures.app_fixtures import AuthenticatedTestClient

    with patch("dbrx_api.dependencies.check_workspace_reachable") as mock_reachable:
        mock_reachable.return_value = (True, None)
        with AuthenticatedTestClient(app_with_workflow) as c:
            yield c
