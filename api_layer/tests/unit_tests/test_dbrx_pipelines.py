"""Test suite for DLT Pipeline SDK functions."""

from unittest.mock import patch

from databricks.sdk.service.pipelines import PipelineState


class TestGetPipelineByName:
    """Tests for get_pipeline_by_name SDK function."""

    def test_get_pipeline_by_name_success(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
    ):
        """Test successful retrieval of pipeline by name."""
        from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name

        # Setup
        pipeline_state_info = mock_pipeline_state_info(pipeline_name="test-pipeline")
        expected_pipeline = mock_get_pipeline_response(pipeline_name="test-pipeline")

        # Mock list_pipelines to return PipelineStateInfo
        mock_workspace_client_pipelines.pipelines.list_pipelines.return_value = [pipeline_state_info]
        # Mock get to return GetPipelineResponse
        mock_workspace_client_pipelines.pipelines.get.return_value = expected_pipeline

        # Execute
        result = get_pipeline_by_name("https://test.databricks.net", "test-pipeline")

        # Assert
        assert result == expected_pipeline
        assert result.name == "test-pipeline"
        mock_workspace_client_pipelines.pipelines.list_pipelines.assert_called_once()
        mock_workspace_client_pipelines.pipelines.get.assert_called_once_with(pipeline_id="test-pipeline-id-123")

    def test_get_pipeline_by_name_not_found(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test retrieval when pipeline doesn't exist."""
        from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name

        # Setup - empty list (no pipeline found)
        mock_workspace_client_pipelines.pipelines.list_pipelines.return_value = iter([])

        # Execute
        result = get_pipeline_by_name("https://test.databricks.net", "nonexistent-pipeline")

        # Assert
        assert result is None

    def test_get_pipeline_by_name_exception(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test handling of exceptions during pipeline retrieval."""
        from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name

        # Setup - raise exception with "not found" message
        mock_workspace_client_pipelines.pipelines.list_pipelines.side_effect = Exception("Pipeline does not exist")

        # Execute
        result = get_pipeline_by_name("https://test.databricks.net", "test-pipeline")

        # Assert - returns None on exception with "not found" in message
        assert result is None


class TestUpdatePipelineContinuous:
    """Tests for update_pipeline_continuous SDK function."""

    def test_update_continuous_mode_to_true(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
    ):
        """Test updating pipeline to continuous mode."""
        from dbrx_api.jobs.dbrx_pipelines import update_pipeline_continuous

        # Setup
        existing_pipeline = mock_pipeline_state_info()
        full_pipeline = mock_get_pipeline_response(continuous=False)

        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            mock_get.return_value = existing_pipeline
            mock_workspace_client_pipelines.pipelines.get.return_value = full_pipeline
            mock_workspace_client_pipelines.pipelines.update.return_value = None

            # Execute
            result = update_pipeline_continuous(
                "https://test.databricks.net",
                "test-pipeline",
                continuous=True,
            )

            # Assert
            assert result is None
            mock_workspace_client_pipelines.pipelines.update.assert_called_once()

            # Verify update was called with continuous=True
            call_kwargs = mock_workspace_client_pipelines.pipelines.update.call_args[1]
            assert call_kwargs["continuous"] is True
            assert "configuration" in call_kwargs  # Settings preserved
            assert "catalog" in call_kwargs
            assert "target" in call_kwargs

    def test_update_continuous_mode_pipeline_not_found(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test updating continuous mode when pipeline doesn't exist."""
        from dbrx_api.jobs.dbrx_pipelines import update_pipeline_continuous

        # Setup
        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            mock_get.return_value = None

            # Execute
            result = update_pipeline_continuous(
                "https://test.databricks.net",
                "nonexistent-pipeline",
                continuous=True,
            )

            # Assert
            assert isinstance(result, str)
            assert "not found" in result.lower()

    def test_update_continuous_preserves_all_settings(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
        mock_pipeline_notifications,
        sample_pipeline_config,
    ):
        """Test that update preserves all pipeline settings."""
        from dbrx_api.jobs.dbrx_pipelines import update_pipeline_continuous

        # Setup with all settings
        existing_pipeline = mock_pipeline_state_info()
        full_pipeline = mock_get_pipeline_response(
            configuration=sample_pipeline_config,
            notifications=mock_pipeline_notifications(),
            serverless=True,
        )

        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            mock_get.return_value = existing_pipeline
            mock_workspace_client_pipelines.pipelines.get.return_value = full_pipeline
            mock_workspace_client_pipelines.pipelines.update.return_value = None

            # Execute
            result = update_pipeline_continuous(
                "https://test.databricks.net",
                "test-pipeline",
                continuous=False,
            )

            # Assert
            assert result is None
            mock_workspace_client_pipelines.pipelines.update.assert_called_once()

            # Assert all settings are preserved
            call_kwargs = mock_workspace_client_pipelines.pipelines.update.call_args[1]
            assert call_kwargs["configuration"] == sample_pipeline_config
            assert call_kwargs["catalog"] == "dltshr_prod"
            assert call_kwargs["target"] == "02_silver"
            assert call_kwargs["serverless"] is True
            assert call_kwargs["notifications"] is not None


class TestPipelineFullRefresh:
    """Tests for pipeline_full_refresh SDK function."""

    def test_full_refresh_idle_pipeline(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
        mock_start_update_response,
    ):
        """Test full refresh on idle pipeline (immediate start)."""
        from dbrx_api.jobs.dbrx_pipelines import pipeline_full_refresh

        # Setup
        existing_pipeline = mock_pipeline_state_info(state=PipelineState.IDLE)
        full_pipeline = mock_get_pipeline_response(state=PipelineState.IDLE)
        start_response = mock_start_update_response()

        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            mock_get.return_value = existing_pipeline
            mock_workspace_client_pipelines.pipelines.get.return_value = full_pipeline
            mock_workspace_client_pipelines.pipelines.start_update.return_value = start_response

            # Execute
            result = pipeline_full_refresh(
                "https://test.databricks.net",
                "test-pipeline",
            )

            # Assert
            assert hasattr(result, "update_id")
            mock_workspace_client_pipelines.pipelines.stop.assert_not_called()
            mock_workspace_client_pipelines.pipelines.start_update.assert_called_once_with(
                pipeline_id="test-pipeline-id-123",
                full_refresh=True,
            )

    def test_full_refresh_running_pipeline_success(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
        mock_start_update_response,
    ):
        """Test full refresh on running pipeline (stop then start)."""
        from dbrx_api.jobs.dbrx_pipelines import pipeline_full_refresh

        # Setup
        existing_pipeline = mock_pipeline_state_info(state=PipelineState.RUNNING)
        full_pipeline_running = mock_get_pipeline_response(state=PipelineState.RUNNING)
        full_pipeline_idle = mock_get_pipeline_response(state=PipelineState.IDLE)

        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            with patch("time.sleep"):  # Mock sleep to speed up test
                mock_get.return_value = existing_pipeline
                # First get returns RUNNING, second get returns IDLE
                mock_workspace_client_pipelines.pipelines.get.side_effect = [
                    full_pipeline_running,
                    full_pipeline_idle,
                ]
                mock_workspace_client_pipelines.pipelines.stop.return_value = None
                mock_workspace_client_pipelines.pipelines.start_update.return_value = mock_start_update_response()

                # Execute
                result = pipeline_full_refresh(
                    "https://test.databricks.net",
                    "test-pipeline",
                )

                # Assert
                assert hasattr(result, "update_id")
                mock_workspace_client_pipelines.pipelines.stop.assert_called_once()
                mock_workspace_client_pipelines.pipelines.start_update.assert_called_once()

    def test_full_refresh_timeout(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
        mock_pipeline_state_info,
        mock_get_pipeline_response,
    ):
        """Test full refresh timeout when pipeline doesn't stop."""
        from dbrx_api.jobs.dbrx_pipelines import pipeline_full_refresh

        # Setup - pipeline stays in STOPPING state
        existing_pipeline = mock_pipeline_state_info(state=PipelineState.RUNNING)
        full_pipeline_stopping = mock_get_pipeline_response(state=PipelineState.STOPPING)

        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            with patch("time.sleep"):  # Mock sleep
                mock_get.return_value = existing_pipeline
                # Always return STOPPING state (timeout scenario)
                mock_workspace_client_pipelines.pipelines.get.return_value = full_pipeline_stopping
                mock_workspace_client_pipelines.pipelines.stop.return_value = None

                # Execute
                result = pipeline_full_refresh(
                    "https://test.databricks.net",
                    "test-pipeline",
                )

                # Assert
                assert isinstance(result, str)
                assert "did not stop within 600 seconds" in result
                assert "STOPPING" in result

    def test_full_refresh_pipeline_not_found(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test full refresh when pipeline doesn't exist."""
        from dbrx_api.jobs.dbrx_pipelines import pipeline_full_refresh

        # Setup
        with patch("dbrx_api.jobs.dbrx_pipelines.get_pipeline_by_name") as mock_get:
            mock_get.return_value = None

            # Execute
            result = pipeline_full_refresh(
                "https://test.databricks.net",
                "nonexistent-pipeline",
            )

            # Assert
            assert isinstance(result, str)
            assert "not found" in result.lower()


class TestDeletePipeline:
    """Tests for delete_pipeline SDK function."""

    def test_delete_pipeline_success(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test successful pipeline deletion."""
        from dbrx_api.jobs.dbrx_pipelines import delete_pipeline

        # Setup
        mock_workspace_client_pipelines.pipelines.delete.return_value = None

        # Execute
        result = delete_pipeline(
            "https://test.databricks.net",
            "test-pipeline-id-123",
        )

        # Assert
        assert result is None
        mock_workspace_client_pipelines.pipelines.delete.assert_called_once_with(pipeline_id="test-pipeline-id-123")

    def test_delete_pipeline_permission_error(
        self,
        mock_auth_token,
        mock_workspace_client_pipelines,
    ):
        """Test pipeline deletion with permission error."""
        from dbrx_api.jobs.dbrx_pipelines import delete_pipeline

        # Setup - make delete raise an exception
        error_msg = "User is not an owner of the pipeline"
        mock_workspace_client_pipelines.pipelines.delete.side_effect = Exception(error_msg)

        # Execute
        result = delete_pipeline(
            "https://test.databricks.net",
            "test-pipeline-id-123",
        )

        # Assert
        assert isinstance(result, str)
        assert "owner" in result.lower() or "permission" in result.lower()
