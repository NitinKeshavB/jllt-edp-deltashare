"""Fixtures for DLT Pipeline testing."""

from typing import Any
from typing import Dict
from typing import List
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service.pipelines import CreatePipelineResponse
from databricks.sdk.service.pipelines import FileLibrary
from databricks.sdk.service.pipelines import GetPipelineResponse
from databricks.sdk.service.pipelines import Notifications
from databricks.sdk.service.pipelines import PipelineCluster
from databricks.sdk.service.pipelines import PipelineLibrary
from databricks.sdk.service.pipelines import PipelineSpec
from databricks.sdk.service.pipelines import PipelineState
from databricks.sdk.service.pipelines import PipelineStateInfo
from databricks.sdk.service.pipelines import StartUpdateResponse


@pytest.fixture
def mock_pipeline_state_info():
    """Create a mock PipelineStateInfo object (lightweight pipeline info)."""

    def _create_pipeline_state(
        pipeline_id: str = "test-pipeline-id-123",
        pipeline_name: str = "test-pipeline",
        state: PipelineState = PipelineState.IDLE,
        creator_user_name: str = "test_user@example.com",
        latest_updates: List[Any] | None = None,
    ) -> PipelineStateInfo:
        """Factory function to create PipelineStateInfo instances."""
        return PipelineStateInfo(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            state=state,
            creator_user_name=creator_user_name,
            latest_updates=latest_updates or [],
        )

    return _create_pipeline_state


@pytest.fixture
def mock_pipeline_spec():
    """Create a mock pipeline spec object."""

    def _create_spec(
        catalog: str = "dltshr_prod",
        target: str = "02_silver",
        configuration: Dict[str, str] | None = None,
        libraries: List[PipelineLibrary] | None = None,
        storage: str = "/mnt/datalake/pipelines/test-pipeline",
        serverless: bool = True,
        development: bool = False,
        continuous: bool = False,
        notifications: Notifications | None = None,
        clusters: List[PipelineCluster] | None = None,
    ):
        """Factory function to create pipeline spec."""
        default_config = {
            "pipelines.keys": "ride_id",
            "pipelines.target_table": "rides_scd",
            "pipelines.sequence_by": "timestamp",
            "pipelines.delete_expr": "is_deleted = true",
        }

        default_libraries = [PipelineLibrary(notebook=FileLibrary(path="/Workspace/pipelines/etl.py"))]

        # Create a real PipelineSpec object instead of MagicMock
        # to avoid Pydantic serialization warnings
        spec = PipelineSpec(
            catalog=catalog,
            target=target,
            configuration=configuration or default_config,
            libraries=libraries or default_libraries,
            storage=storage,
            serverless=serverless,
            development=development,
            continuous=continuous,
            notifications=notifications,
            clusters=clusters,
        )

        return spec

    return _create_spec


@pytest.fixture
def mock_get_pipeline_response(mock_pipeline_spec):
    """Create a mock GetPipelineResponse object (full pipeline details)."""

    def _create_full_pipeline(
        pipeline_id: str = "test-pipeline-id-123",
        pipeline_name: str = "test-pipeline",
        state: PipelineState = PipelineState.IDLE,
        creator_user_name: str = "test_user@example.com",
        **spec_kwargs,
    ) -> GetPipelineResponse:
        """Factory function to create GetPipelineResponse instances."""
        # Create a real GetPipelineResponse object instead of MagicMock
        # to avoid Pydantic serialization warnings
        response = GetPipelineResponse(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            state=state,
            creator_user_name=creator_user_name,
            spec=mock_pipeline_spec(**spec_kwargs),
            # Optional fields with defaults
            cluster_id=None,
            health=None,
            latest_updates=[],
            run_as_user_name=None,
            cause=None,
            last_modified=None,
        )

        return response

    return _create_full_pipeline


@pytest.fixture
def mock_create_pipeline_response():
    """Create a mock CreatePipelineResponse object."""

    def _create_response(
        pipeline_id: str = "new-pipeline-id-456",
        pipeline_name: str = "new-pipeline",
    ) -> CreatePipelineResponse:
        """Factory function to create CreatePipelineResponse instances."""
        response = MagicMock(spec=CreatePipelineResponse)
        response.pipeline_id = pipeline_id
        response.name = pipeline_name
        return response

    return _create_response


@pytest.fixture
def mock_start_update_response():
    """Create a mock StartUpdateResponse object."""

    def _create_response(
        update_id: str = "update-id-789",
    ) -> StartUpdateResponse:
        """Factory function to create StartUpdateResponse instances."""
        response = MagicMock(spec=StartUpdateResponse)
        response.update_id = update_id
        return response

    return _create_response


@pytest.fixture
def mock_pipeline_notifications():
    """Create a mock Notifications object."""

    def _create_notifications(
        email_recipients: List[str] | None = None,
        alerts: List[str] | None = None,
    ) -> Notifications:
        """Factory function to create Notifications instances."""
        return Notifications(
            email_recipients=email_recipients or ["team@example.com"],
            alerts=alerts or ["on_update_failure", "on_update_success"],
        )

    return _create_notifications


@pytest.fixture
def mock_pipeline_cluster():
    """Create a mock PipelineCluster object."""

    def _create_cluster(
        custom_tags: Dict[str, str] | None = None,
    ) -> PipelineCluster:
        """Factory function to create PipelineCluster instances."""
        return PipelineCluster(
            custom_tags=custom_tags
            or {
                "env": "prod",
                "team": "data-engineering",
                "project": "deltashare",
            }
        )

    return _create_cluster


@pytest.fixture
def mock_pipelines_api(
    mock_pipeline_state_info,
    mock_get_pipeline_response,
    mock_create_pipeline_response,
    mock_start_update_response,
):
    """Mock Databricks Pipelines API."""
    mock_api = MagicMock()

    # Mock list_pipelines
    mock_api.list_pipelines.return_value = [
        mock_pipeline_state_info(pipeline_name="pipeline1"),
        mock_pipeline_state_info(pipeline_name="pipeline2"),
    ]

    # Mock get
    mock_api.get.return_value = mock_get_pipeline_response()

    # Mock create
    mock_api.create.return_value = mock_create_pipeline_response()

    # Mock update
    mock_api.update.return_value = None

    # Mock delete
    mock_api.delete.return_value = None

    # Mock stop
    mock_api.stop.return_value = None

    # Mock start_update
    mock_api.start_update.return_value = mock_start_update_response()

    return mock_api


@pytest.fixture
def mock_workspace_client_pipelines(mock_pipelines_api):
    """Mock Databricks WorkspaceClient and get_auth_token for pipeline SDK tests."""
    from datetime import datetime
    from datetime import timezone

    token_return = ("test-token", datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
    with patch("dbrx_api.jobs.dbrx_pipelines.get_auth_token") as mock_token:
        mock_token.return_value = token_return
        with patch("dbrx_api.jobs.dbrx_pipelines.WorkspaceClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client.pipelines = mock_pipelines_api
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def mock_get_pipeline_by_name():
    """Mock get_pipeline_by_name SDK function."""
    with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock:
        yield mock


@pytest.fixture
def sample_pipeline_config():
    """Sample pipeline configuration dictionary."""
    return {
        "pipelines.keys": "ride_id",
        "pipelines.target_table": "rides_scd_type2",
        "pipelines.sequence_by": "timestamp",
        "pipelines.delete_expr": "is_deleted = true",
    }


@pytest.fixture
def sample_create_pipeline_request():
    """Sample CreatePipelineRequest payload."""
    return {
        "pipeline_name": "test-dlt-pipeline",
        "target_catalog_name": "dltshr_prod",
        "target_schema_name": "02_silver",
        "configuration": {
            "pipelines.source_table": "dltshr_prod.01_bronze.source_data",
            "pipelines.keys": "ride_id",
            "pipelines.target_table": "rides_scd",
            "pipelines.scd_type": "2",
        },
        "notifications_list": [
            "data-team@example.com",
            "admin@example.com",
        ],
        "tags": {
            "env": "prod",
            "team": "data-engineering",
            "project": "deltashare",
        },
        "serverless": True,
    }


@pytest.fixture
def sample_update_continuous_request():
    """Sample UpdatePipelineContinuousModel payload."""
    return {"continuous": True}


@pytest.fixture
def sample_pipeline_tags():
    """Sample pipeline tags."""
    return {
        "env": "prod",
        "team": "data-engineering",
        "project": "deltashare",
        "cost_center": "analytics",
        "owner": "admin@example.com",
    }
