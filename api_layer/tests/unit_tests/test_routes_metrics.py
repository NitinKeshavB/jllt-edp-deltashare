"""Test suite for Metrics API endpoints."""

from unittest.mock import patch

from fastapi import status
from tests.consts import API_BASE


class TestGetPipelineRunMetricsEndpoint:
    """Tests for GET /api/pipelines/{pipeline_name}/metrics endpoint."""

    def test_get_pipeline_metrics_success(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test successfully getting pipeline run metrics."""
        runs_list = [
            {
                "update_id": "1",
                "run_status": "COMPLETED",
                "start_time": "2024-01-23T10:00:00Z",
                "end_time": "2024-01-23T10:15:00Z",
                "duration_seconds": 900,
            },
        ]
        with (
            patch("dbrx_api.routes.routes_metrics.get_pipeline_by_name_sdk") as mock_get,
            patch("dbrx_api.routes.routes_metrics.get_pipeline_metrics_sdk") as mock_metrics,
        ):
            mock_get.return_value = mock_pipeline_state_info()
            mock_metrics.return_value = runs_list

            response = client.get(f"{API_BASE}/pipelines/test-pipeline/metrics")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["pipeline_name"] == "test-pipeline"
            assert "runs" in data
            assert data["total_runs"] == len(runs_list)
            assert len(data["runs"]) == len(runs_list)

    def test_get_pipeline_metrics_pipeline_not_found(
        self,
        client,
        mock_auth_token,
    ):
        """Test pipeline metrics when pipeline does not exist."""
        with patch("dbrx_api.routes.routes_metrics.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = None

            response = client.get(f"{API_BASE}/pipelines/nonexistent-pipeline/metrics")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_get_pipeline_metrics_invalid_timestamp(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test pipeline metrics with invalid timestamp returns 400."""
        with (
            patch("dbrx_api.routes.routes_metrics.get_pipeline_by_name_sdk") as mock_get,
            patch("dbrx_api.routes.routes_metrics.get_pipeline_metrics_sdk") as mock_metrics,
        ):
            mock_get.return_value = mock_pipeline_state_info()
            mock_metrics.return_value = "Invalid timestamp format. Expected ISO format."

            response = client.get(
                f"{API_BASE}/pipelines/test-pipeline/metrics",
                params={"start_timestamp": "not-a-date"},
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "timestamp" in response.json()["detail"].lower()


class TestGetJobRunMetricsEndpoint:
    """Tests for GET /api/pipelines/{pipeline_name}/job-runs/metrics endpoint."""

    def test_get_job_run_metrics_success(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test successfully getting job run metrics."""
        runs_list = [
            {
                "job_name": "scheduled-job",
                "run_id": 67890,
                "run_status": "TERMINATED",
                "result_state": "SUCCESS",
            },
        ]
        with (
            patch("dbrx_api.routes.routes_metrics.get_pipeline_by_name_sdk") as mock_get,
            patch("dbrx_api.routes.routes_metrics.get_job_run_metrics_sdk") as mock_metrics,
        ):
            mock_get.return_value = mock_pipeline_state_info()
            mock_metrics.return_value = runs_list

            response = client.get(f"{API_BASE}/pipelines/test-pipeline/job-runs/metrics")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["pipeline_name"] == "test-pipeline"
            assert "runs" in data
            assert data["total_runs"] == len(runs_list)

    def test_get_job_run_metrics_pipeline_not_found(
        self,
        client,
        mock_auth_token,
    ):
        """Test job run metrics when pipeline does not exist."""
        with patch("dbrx_api.routes.routes_metrics.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = None

            response = client.get(f"{API_BASE}/pipelines/nonexistent-pipeline/job-runs/metrics")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()
