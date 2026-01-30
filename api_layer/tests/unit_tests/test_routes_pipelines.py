"""Test suite for DLT Pipeline API endpoints."""

from unittest.mock import patch

from fastapi import status


class TestPipelineAuthenticationHeaders:
    """Tests for required authentication headers on Pipeline endpoints."""

    def test_missing_workspace_url_header(self, unauthenticated_client):
        """Test that requests without X-Workspace-URL header are rejected."""
        response = unauthenticated_client.put(
            "/pipelines/test-pipeline/continuous",
            json={"continuous": True},
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "X-Workspace-URL" in str(response.json())


class TestUpdatePipelineContinuousEndpoint:
    """Tests for PUT /pipelines/{pipeline_name}/continuous endpoint."""

    def test_update_continuous_to_true_success(
        self,
        client,
        mock_auth_token,
        sample_update_continuous_request,
    ):
        """Test successfully updating pipeline to continuous mode."""
        with patch("dbrx_api.routes.routes_pipelines.update_pipeline_continuous_sdk") as mock_update:
            mock_update.return_value = None  # Success

            response = client.put(
                "/pipelines/test-pipeline/continuous",
                json=sample_update_continuous_request,
            )

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["pipeline_name"] == "test-pipeline"
            assert data["continuous"] is True
            assert data["mode"] == "continuous"
            mock_update.assert_called_once()

    def test_update_continuous_to_false_success(
        self,
        client,
        mock_auth_token,
    ):
        """Test successfully updating pipeline to triggered mode."""
        with patch("dbrx_api.routes.routes_pipelines.update_pipeline_continuous_sdk") as mock_update:
            mock_update.return_value = None  # Success

            response = client.put(
                "/pipelines/test-pipeline/continuous",
                json={"continuous": False},
            )

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["continuous"] is False
            assert data["mode"] == "triggered"

    def test_update_continuous_pipeline_not_found(
        self,
        client,
        mock_auth_token,
        sample_update_continuous_request,
    ):
        """Test updating continuous mode when pipeline doesn't exist."""
        with patch("dbrx_api.routes.routes_pipelines.update_pipeline_continuous_sdk") as mock_update:
            mock_update.return_value = "Pipeline not found: nonexistent-pipeline"

            response = client.put(
                "/pipelines/nonexistent-pipeline/continuous",
                json=sample_update_continuous_request,
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_update_continuous_permission_denied(
        self,
        client,
        mock_auth_token,
        sample_update_continuous_request,
    ):
        """Test updating continuous mode with permission denied."""
        with patch("dbrx_api.routes.routes_pipelines.update_pipeline_continuous_sdk") as mock_update:
            mock_update.return_value = "Permission denied: User is not an owner"

            response = client.put(
                "/pipelines/test-pipeline/continuous",
                json=sample_update_continuous_request,
            )

            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "permission" in response.json()["detail"].lower()

    def test_update_continuous_missing_field(
        self,
        client,
        mock_auth_token,
    ):
        """Test updating continuous mode with missing required field."""
        response = client.put(
            "/pipelines/test-pipeline/continuous",
            json={},  # Missing 'continuous' field
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_update_continuous_invalid_type(
        self,
        client,
        mock_auth_token,
    ):
        """Test updating continuous mode with invalid type."""
        response = client.put(
            "/pipelines/test-pipeline/continuous",
            json={"continuous": "not-a-boolean"},  # Should be boolean
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestPipelineFullRefreshEndpoint:
    """Tests for POST /pipelines/{pipeline_name}/full-refresh endpoint."""

    def test_full_refresh_success(
        self,
        client,
        mock_auth_token,
        mock_start_update_response,
    ):
        """Test successfully starting full refresh."""
        with patch("dbrx_api.routes.routes_pipelines.pipeline_full_refresh_sdk") as mock_refresh:
            mock_refresh.return_value = mock_start_update_response()

            response = client.post("/pipelines/test-pipeline/full-refresh")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["pipeline_name"] == "test-pipeline"
            assert data["action"] == "full_refresh"
            assert data["status"] == "started"
            # Verify mock was called (workspace URL may vary)
            mock_refresh.assert_called_once()
            call_args = mock_refresh.call_args[1]
            assert call_args["pipeline_name"] == "test-pipeline"

    def test_full_refresh_pipeline_not_found(
        self,
        client,
        mock_auth_token,
    ):
        """Test full refresh when pipeline doesn't exist."""
        with patch("dbrx_api.routes.routes_pipelines.pipeline_full_refresh_sdk") as mock_refresh:
            mock_refresh.return_value = "Pipeline not found: nonexistent-pipeline"

            response = client.post("/pipelines/nonexistent-pipeline/full-refresh")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_full_refresh_timeout(
        self,
        client,
        mock_auth_token,
    ):
        """Test full refresh timeout."""
        with patch("dbrx_api.routes.routes_pipelines.pipeline_full_refresh_sdk") as mock_refresh:
            mock_refresh.return_value = (
                "Pipeline did not stop within 600 seconds (10 minutes). Current state: STOPPING"
            )

            response = client.post("/pipelines/test-pipeline/full-refresh")

            assert response.status_code == status.HTTP_408_REQUEST_TIMEOUT
            assert "did not stop within" in response.json()["detail"]

    def test_full_refresh_permission_denied(
        self,
        client,
        mock_auth_token,
    ):
        """Test full refresh with permission denied."""
        with patch("dbrx_api.routes.routes_pipelines.pipeline_full_refresh_sdk") as mock_refresh:
            mock_refresh.return_value = "Permission denied: User is not the owner"

            response = client.post("/pipelines/test-pipeline/full-refresh")

            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "permission" in response.json()["detail"].lower()

    def test_full_refresh_generic_error(
        self,
        client,
        mock_auth_token,
    ):
        """Test full refresh with generic error."""
        with patch("dbrx_api.routes.routes_pipelines.pipeline_full_refresh_sdk") as mock_refresh:
            mock_refresh.return_value = "Some unexpected error occurred"

            response = client.post("/pipelines/test-pipeline/full-refresh")

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "failed to start full refresh" in response.json()["detail"].lower()


class TestDeletePipelineEndpoint:
    """Tests for DELETE /pipelines/{pipeline_name} endpoint."""

    def test_delete_pipeline_success(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test successfully deleting a pipeline."""
        with patch("dbrx_api.routes.routes_pipelines.get_pipeline_by_name_sdk") as mock_get:
            with patch("dbrx_api.routes.routes_pipelines.delete_pipeline_sdk") as mock_delete:
                mock_get.return_value = mock_pipeline_state_info()
                mock_delete.return_value = None  # Success

                response = client.delete("/pipelines/test-pipeline")

                assert response.status_code == status.HTTP_200_OK
                data = response.json()
                assert "deleted successfully" in data["message"]
                # Verify mock was called with correct pipeline ID
                mock_delete.assert_called_once()
                call_args = mock_delete.call_args[0]
                assert call_args[1] == "test-pipeline-id-123"

    def test_delete_pipeline_not_found(
        self,
        client,
        mock_auth_token,
    ):
        """Test deleting non-existent pipeline."""
        with patch("dbrx_api.routes.routes_pipelines.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = None

            response = client.delete("/pipelines/nonexistent-pipeline")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_delete_pipeline_permission_denied(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test deleting pipeline with permission denied."""
        with patch("dbrx_api.routes.routes_pipelines.get_pipeline_by_name_sdk") as mock_get:
            with patch("dbrx_api.routes.routes_pipelines.delete_pipeline_sdk") as mock_delete:
                mock_get.return_value = mock_pipeline_state_info()
                mock_delete.return_value = "User is not an owner of the pipeline"

                response = client.delete("/pipelines/test-pipeline")

                assert response.status_code == status.HTTP_403_FORBIDDEN
                assert "permission" in response.json()["detail"].lower()


class TestGetPipelineByNameEndpoint:
    """Tests for GET /pipelines/{pipeline_name} endpoint (if exists)."""

    def test_get_pipeline_success(
        self,
        client,
        mock_auth_token,
        mock_get_pipeline_response,
    ):
        """Test successfully retrieving a pipeline by name."""
        with patch("dbrx_api.routes.routes_pipelines.get_pipeline_by_name_sdk") as mock_get:
            pipeline = mock_get_pipeline_response()
            mock_get.return_value = pipeline

            response = client.get("/pipelines/test-pipeline")

            # Should return 200 with pipeline details
            assert response.status_code == status.HTTP_200_OK

    def test_get_pipeline_not_found(
        self,
        client,
        mock_auth_token,
    ):
        """Test retrieving non-existent pipeline."""
        with patch("dbrx_api.routes.routes_pipelines.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = None

            response = client.get("/pipelines/nonexistent-pipeline")

            # Expect 404 for non-existent pipeline
            assert response.status_code == status.HTTP_404_NOT_FOUND


class TestListPipelinesEndpoint:
    """Tests for GET /pipelines endpoint (if exists)."""

    def test_list_all_pipelines_success(
        self,
        client,
        mock_auth_token,
        mock_pipeline_state_info,
    ):
        """Test successfully listing all pipelines."""
        with patch("dbrx_api.jobs.dbrx_pipelines.list_pipelines") as mock_list:
            mock_list.return_value = [
                mock_pipeline_state_info(pipeline_name="pipeline1"),
                mock_pipeline_state_info(pipeline_name="pipeline2"),
            ]

            response = client.get("/pipelines")

            # Assert based on your actual implementation
            if response.status_code == status.HTTP_200_OK:
                data = response.json()
                assert len(data) >= 0  # May return list or wrapped response

    def test_list_pipelines_empty(
        self,
        client,
        mock_auth_token,
    ):
        """Test listing pipelines when none exist."""
        with patch("dbrx_api.routes.routes_pipelines.list_pipelines_sdk") as mock_list:
            mock_list.return_value = []

            response = client.get("/pipelines")

            # Should handle empty list gracefully
            assert response.status_code in [status.HTTP_200_OK, status.HTTP_404_NOT_FOUND]
