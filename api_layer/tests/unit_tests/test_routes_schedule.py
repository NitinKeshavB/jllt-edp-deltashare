"""Test suite for Schedule API endpoints."""

from unittest.mock import patch

from fastapi import status

from tests.consts import API_BASE


class TestScheduleAuthenticationHeaders:
    """Tests for required authentication headers on Schedule endpoints."""

    def test_missing_workspace_url_header(self, unauthenticated_client):
        """Test that requests without X-Workspace-URL header are rejected."""
        response = unauthenticated_client.get(f"{API_BASE}/schedules")
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestListAllSchedulesEndpoint:
    """Tests for GET /schedules endpoint."""

    def test_list_all_schedules_success(
        self,
        client,
        sample_schedule_list,
    ):
        """Test successfully listing all schedules."""
        with patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list:
            mock_list.return_value = (sample_schedule_list, None)

            response = client.get(f"{API_BASE}/schedules")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["total"] == 3
            assert len(data["schedules"]) == 3

    def test_list_all_schedules_auto_pagination(
        self,
        client,
        sample_schedule_list,
    ):
        """Test auto-pagination fetches all schedules across multiple pages."""
        with patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list:
            # Simulate multiple pages: first call returns 2 items with token, second returns 1 item with no token
            mock_list.side_effect = [
                (sample_schedule_list[:2], "next-page-token"),
                (sample_schedule_list[2:], None),
            ]

            response = client.get(f"{API_BASE}/schedules")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["total"] == 3
            assert len(data["schedules"]) == 3
            # Response should not include pagination fields (auto-paginated)
            assert "has_more" not in data
            assert "next_page_token" not in data
            # Verify SDK was called twice for pagination
            assert mock_list.call_count == 2

    def test_list_all_schedules_custom_page_size(
        self,
        client,
        sample_schedule_list,
    ):
        """Test custom page_size parameter for internal batching."""
        with patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list:
            mock_list.return_value = (sample_schedule_list, None)

            response = client.get(f"{API_BASE}/schedules?page_size=50")

            assert response.status_code == status.HTTP_200_OK
            # Verify SDK was called with custom page_size (max_results)
            mock_list.assert_called_once()
            call_kwargs = mock_list.call_args[1]
            assert call_kwargs["max_results"] == 50

    def test_list_all_schedules_invalid_page_size(self, client):
        """Test that page_size=-1 is no longer valid."""
        response = client.get(f"{API_BASE}/schedules?page_size=-1")

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_list_all_schedules_filter_by_pipeline(
        self,
        client,
        sample_schedule_list,
        mock_pipeline_for_schedule,
    ):
        """Test filtering schedules by pipeline name."""
        with (
            patch("dbrx_api.routes.routes_schedule.list_pipelines_with_search_sdk") as mock_search,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_search.return_value = [mock_pipeline_for_schedule]
            mock_list.return_value = (sample_schedule_list[:1], None)

            response = client.get(f"{API_BASE}/schedules?pipeline_name_search_string=test-pipeline")

            assert response.status_code == status.HTTP_200_OK

    def test_list_all_schedules_no_matching_pipeline(self, client, mock_auth_token):
        """Test filtering by non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.list_pipelines_with_search_sdk") as mock_search, patch(
            "dbrx_api.routes.routes_schedule.list_schedules_sdk"
        ) as mock_list:
            mock_search.return_value = []
            # Mock list_schedules_sdk to avoid real API calls (shouldn't be called but defensive)
            mock_list.return_value = ([], None)

            response = client.get(f"{API_BASE}/schedules?pipeline_name_search_string=nonexistent")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["total"] == 0
            # Verify list_schedules_sdk is NOT called when no pipelines match
            mock_list.assert_not_called()


class TestListSchedulesForPipelineEndpoint:
    """Tests for GET /schedules/pipeline/{pipeline_name} endpoint."""

    def test_list_schedules_success(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_schedule_list,
    ):
        """Test successfully listing schedules for a pipeline."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = (sample_schedule_list, None)

            response = client.get(f"{API_BASE}/schedules/pipeline/test-pipeline")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["pipeline_name"] == "test-pipeline"
            assert data["total"] == 3
            assert len(data["schedules"]) == 3
            # Response should not include pagination fields (auto-paginated)
            assert "has_more" not in data
            assert "next_page_token" not in data

    def test_list_schedules_auto_pagination(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_schedule_list,
    ):
        """Test auto-pagination fetches all schedules for pipeline across multiple pages."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            # Simulate multiple pages
            mock_list.side_effect = [
                (sample_schedule_list[:2], "next-page-token"),
                (sample_schedule_list[2:], None),
            ]

            response = client.get(f"{API_BASE}/schedules/pipeline/test-pipeline")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["total"] == 3
            assert len(data["schedules"]) == 3
            # Verify SDK was called twice for pagination
            assert mock_list.call_count == 2

    def test_list_schedules_empty(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test listing schedules when none exist."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)

            response = client.get(f"{API_BASE}/schedules/pipeline/test-pipeline")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["total"] == 0
            assert data["schedules"] == []

    def test_list_schedules_pipeline_not_found(self, client):
        """Test listing schedules for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.get(f"{API_BASE}/schedules/pipeline/nonexistent-pipeline")

            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_list_schedules_invalid_page_size(self, client, mock_pipeline_for_schedule):
        """Test that page_size=-1 is no longer valid for pipeline schedules."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = mock_pipeline_for_schedule

            response = client.get(f"{API_BASE}/schedules/pipeline/test-pipeline?page_size=-1")

            assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestCreateScheduleEndpoint:
    """Tests for POST /pipelines/{pipeline_name}/schedules endpoint."""

    def test_create_schedule_success(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_create_schedule_request,
    ):
        """Test successfully creating a schedule."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)
            mock_create.return_value = "Schedule created successfully"

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json=sample_create_schedule_request,
            )

            assert response.status_code == status.HTTP_201_CREATED
            data = response.json()
            assert data["message"] == "Schedule created successfully"
            assert data["job_name"] == sample_create_schedule_request["job_name"]

    def test_create_schedule_minimal_request(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_create_schedule_request_minimal,
    ):
        """Test creating a schedule with minimal required fields."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)
            mock_create.return_value = "Schedule created successfully"

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json=sample_create_schedule_request_minimal,
            )

            assert response.status_code == status.HTTP_201_CREATED

    def test_create_schedule_pipeline_not_found(
        self,
        client,
        sample_create_schedule_request,
    ):
        """Test creating schedule for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.post(
                f"{API_BASE}/pipelines/nonexistent-pipeline/schedules",
                json=sample_create_schedule_request,
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_schedule_duplicate_cron(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_create_schedule_request,
        mock_schedule_info,
    ):
        """Test creating schedule with duplicate cron expression."""
        existing_schedule = mock_schedule_info(cron_expression=sample_create_schedule_request["cron_expression"])

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([existing_schedule], None)

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json=sample_create_schedule_request,
            )

            assert response.status_code == status.HTTP_409_CONFLICT
            assert "already exists" in response.json()["detail"].lower()

    def test_create_schedule_job_already_exists(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_create_schedule_request,
    ):
        """Test creating schedule when job name already exists."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)
            mock_create.return_value = "Job already exists: new-schedule-job"

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json=sample_create_schedule_request,
            )

            assert response.status_code == status.HTTP_409_CONFLICT

    def test_create_schedule_invalid_cron(self, client, mock_pipeline_for_schedule):
        """Test creating schedule with invalid cron expression."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = mock_pipeline_for_schedule

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json={
                    "job_name": "test-job",
                    "cron_expression": "invalid cron",
                },
            )

            assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_schedule_invalid_job_name(self, client, mock_pipeline_for_schedule):
        """Test creating schedule with invalid job name."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = mock_pipeline_for_schedule

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json={
                    "job_name": "invalid@job#name!",
                    "cron_expression": "0 0 12 * * ?",
                },
            )

            assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_schedule_missing_required_fields(self, client):
        """Test creating schedule without required fields."""
        response = client.post(
            f"{API_BASE}/pipelines/test-pipeline/schedules",
            json={},
        )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestUpdateCronExpressionEndpoint:
    """Tests for PATCH /pipelines/{pipeline_name}/schedules/{job_name}/cron endpoint."""

    def test_update_cron_success(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test successfully updating schedule cron expression."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_cron_expression_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Schedule updated successfully"

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/cron?cron_expression=0 0 6 * * ?"
            )

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["cron_expression"] == "0 0 6 * * ?"

    def test_update_cron_pipeline_not_found(self, client):
        """Test updating cron for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.patch(
                f"{API_BASE}/pipelines/nonexistent-pipeline/schedules/test-job/cron?cron_expression=0 0 6 * * ?"
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_cron_schedule_not_found(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test updating cron for non-existent schedule."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/nonexistent-job/cron?cron_expression=0 0 6 * * ?"
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_cron_invalid_expression(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test updating with invalid cron expression."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = mock_pipeline_for_schedule

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/cron?cron_expression=invalid"
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_update_cron_same_value_conflict(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating cron expression with same value returns 409 conflict."""
        existing_cron = "0 0 12 * * ?"
        schedule = mock_schedule_info(job_name="test-job", job_id="123", cron_expression=existing_cron)

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/cron?cron_expression={existing_cron}"
            )

            assert response.status_code == status.HTTP_409_CONFLICT
            assert "already has cron expression" in response.json()["detail"].lower()


class TestUpdateTimezoneEndpoint:
    """Tests for PATCH /pipelines/{pipeline_name}/schedules/{job_name}/timezone endpoint."""

    def test_update_timezone_success(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test successfully updating schedule timezone."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_timezone_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Timezone updated successfully"

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone?time_zone=America/New_York"
            )

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["time_zone"] == "America/New_York"

    def test_update_timezone_default_utc(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating timezone defaults to UTC when current timezone is different."""
        # Schedule has America/New_York timezone, updating without param should use UTC
        schedule = mock_schedule_info(job_name="test-job", job_id="123", timezone="America/New_York")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_timezone_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Timezone updated successfully"

            response = client.patch(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["time_zone"] == "UTC"

    def test_update_timezone_pipeline_not_found(self, client):
        """Test updating timezone for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.patch(
                f"{API_BASE}/pipelines/nonexistent-pipeline/schedules/test-job/timezone?time_zone=UTC"
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_timezone_schedule_not_found(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test updating timezone for non-existent schedule."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/nonexistent-job/timezone?time_zone=UTC"
            )

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_timezone_same_value_conflict(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating timezone with same value returns 409 conflict."""
        existing_timezone = "America/New_York"
        schedule = mock_schedule_info(job_name="test-job", job_id="123", timezone=existing_timezone)

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone?time_zone={existing_timezone}"
            )

            assert response.status_code == status.HTTP_409_CONFLICT
            assert "already has timezone" in response.json()["detail"].lower()

    def test_update_timezone_same_utc_conflict(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating timezone to UTC when already UTC returns 409 conflict."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123", timezone="UTC")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)

            response = client.patch(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone?time_zone=UTC")

            assert response.status_code == status.HTTP_409_CONFLICT
            assert "already has timezone" in response.json()["detail"].lower()


class TestDeleteScheduleEndpoint:
    """Tests for DELETE /pipelines/{pipeline_name}/schedules/{job_name} endpoint."""

    def test_delete_schedule_success(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test successfully deleting a schedule."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.delete_schedule_for_pipeline_sdk") as mock_delete,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([{"job_name": "test-job", "job_id": "job-123"}], None)
            mock_delete.return_value = "Schedule deleted successfully"

            response = client.delete(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert "deleted" in data["message"].lower()

    def test_delete_schedule_not_found(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test deleting non-existent schedule."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.delete_schedule_for_pipeline_sdk") as mock_delete,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([{"job_name": "test-job", "job_id": "job-123"}], None)
            mock_delete.return_value = "Job not found: test-job"

            response = client.delete(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job")

            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_delete_schedule_permission_denied(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test deleting schedule with permission denied."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.delete_schedule_for_pipeline_sdk") as mock_delete,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([{"job_name": "test-job", "job_id": "job-123"}], None)
            mock_delete.return_value = "Permission denied: not an owner"

            response = client.delete(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job")

            assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_delete_schedule_pipeline_not_found(self, client):
        """Test deleting schedule for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.delete(f"{API_BASE}/pipelines/nonexistent-pipeline/schedules/test-job")

            assert response.status_code == status.HTTP_404_NOT_FOUND


class TestDeleteAllSchedulesEndpoint:
    """Tests for DELETE /pipelines/{pipeline_name}/schedules endpoint."""

    def test_delete_all_schedules_success(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test successfully deleting all schedules."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.delete_schedule_for_pipeline_sdk") as mock_delete,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_delete.return_value = "Deleted 3 schedule(s)"

            response = client.delete(f"{API_BASE}/pipelines/test-pipeline/schedules")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert "deleted" in data["message"].lower()

    def test_delete_all_schedules_none_exist(
        self,
        client,
        mock_pipeline_for_schedule,
    ):
        """Test deleting all schedules when none exist."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.delete_schedule_for_pipeline_sdk") as mock_delete,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_delete.return_value = "No schedules found for pipeline"

            response = client.delete(f"{API_BASE}/pipelines/test-pipeline/schedules")

            assert response.status_code == status.HTTP_200_OK

    def test_delete_all_schedules_pipeline_not_found(self, client):
        """Test deleting all schedules for non-existent pipeline."""
        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline:
            mock_get_pipeline.return_value = None

            response = client.delete(f"{API_BASE}/pipelines/nonexistent-pipeline/schedules")

            assert response.status_code == status.HTTP_404_NOT_FOUND


class TestCreateScheduleRequestValidation:
    """Tests for CreateScheduleRequest Pydantic model validation."""

    def test_valid_cron_expressions(self, client, mock_pipeline_for_schedule):
        """Test various valid Quartz cron expressions."""
        valid_crons = [
            "0 0 12 * * ?",  # Daily at noon
            "0 30 9 ? * MON-FRI",  # Weekdays at 9:30
            "0 0 0 1 * ?",  # First of month
            "0 0 */2 * * ?",  # Every 2 hours
            "0 15 10 ? * *",  # Daily at 10:15
        ]

        for cron in valid_crons:
            with (
                patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get,
                patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
                patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
            ):
                mock_get.return_value = mock_pipeline_for_schedule
                mock_list.return_value = ([], None)
                mock_create.return_value = "Schedule created successfully"

                response = client.post(
                    f"{API_BASE}/pipelines/test-pipeline/schedules",
                    json={"job_name": "test-job", "cron_expression": cron},
                )

                assert response.status_code == status.HTTP_201_CREATED, f"Failed for cron: {cron}"

    def test_invalid_cron_expressions(self, client, mock_pipeline_for_schedule):
        """Test various invalid cron expressions."""
        invalid_crons = [
            "* * * * *",  # Standard cron (5 fields, not Quartz)
            "invalid",  # Not a cron at all
            "0 0 12 * *",  # Only 5 fields
            "",  # Empty
        ]

        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = mock_pipeline_for_schedule

            for cron in invalid_crons:
                response = client.post(
                    f"{API_BASE}/pipelines/test-pipeline/schedules",
                    json={"job_name": "test-job", "cron_expression": cron},
                )

                assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, f"Should fail for cron: {cron}"

    def test_valid_job_names(self, client, mock_pipeline_for_schedule):
        """Test various valid job names."""
        valid_names = [
            "simple-job",
            "job_with_underscores",
            "job.with.dots",
            "Job With Spaces",
            "job123",
        ]

        for name in valid_names:
            with (
                patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get,
                patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
                patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
            ):
                mock_get.return_value = mock_pipeline_for_schedule
                mock_list.return_value = ([], None)
                mock_create.return_value = "Schedule created successfully"

                response = client.post(
                    f"{API_BASE}/pipelines/test-pipeline/schedules",
                    json={"job_name": name, "cron_expression": "0 0 12 * * ?"},
                )

                assert response.status_code == status.HTTP_201_CREATED, f"Failed for name: {name}"

    def test_invalid_job_names(self, client, mock_pipeline_for_schedule):
        """Test various invalid job names."""
        invalid_names = [
            "job@name",  # @ not allowed
            "job#name",  # # not allowed
            "job!name",  # ! not allowed
            "",  # Empty
            "   ",  # Whitespace only
        ]

        with patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get:
            mock_get.return_value = mock_pipeline_for_schedule

            for name in invalid_names:
                response = client.post(
                    f"{API_BASE}/pipelines/test-pipeline/schedules",
                    json={"job_name": name, "cron_expression": "0 0 12 * * ?"},
                )

                assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, f"Should fail for name: {name}"


class TestScheduleErrorHandling:
    """Tests for error handling in schedule endpoints."""

    def test_create_schedule_sdk_error(
        self,
        client,
        mock_pipeline_for_schedule,
        sample_create_schedule_request,
    ):
        """Test create schedule when SDK returns an error."""
        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.create_schedule_for_pipeline_sdk") as mock_create,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([], None)
            mock_create.return_value = "Error: Failed to create schedule due to network issue"

            response = client.post(
                f"{API_BASE}/pipelines/test-pipeline/schedules",
                json=sample_create_schedule_request,
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "error" in response.json()["detail"].lower()

    def test_update_cron_permission_denied(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating cron expression with permission denied."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_cron_expression_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Permission denied: User is not the owner of this job"

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/cron?cron_expression=0 0 6 * * ?"
            )

            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "permission denied" in response.json()["detail"].lower()

    def test_update_cron_sdk_error(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating cron expression when SDK returns an error."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_cron_expression_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Error: Failed to update schedule"

            response = client.patch(
                f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/cron?cron_expression=0 0 6 * * ?"
            )

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "error" in response.json()["detail"].lower()

    def test_update_timezone_permission_denied(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating timezone with permission denied."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123", timezone="America/New_York")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_timezone_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Permission denied: User is not the owner of this job"

            response = client.patch(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone?time_zone=UTC")

            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "permission denied" in response.json()["detail"].lower()

    def test_update_timezone_sdk_error(
        self,
        client,
        mock_pipeline_for_schedule,
        mock_schedule_info,
    ):
        """Test updating timezone when SDK returns an error."""
        schedule = mock_schedule_info(job_name="test-job", job_id="123", timezone="America/New_York")

        with (
            patch("dbrx_api.routes.routes_schedule.get_pipeline_by_name_sdk") as mock_get_pipeline,
            patch("dbrx_api.routes.routes_schedule.list_schedules_sdk") as mock_list,
            patch("dbrx_api.routes.routes_schedule.update_timezone_for_schedule_sdk") as mock_update,
        ):
            mock_get_pipeline.return_value = mock_pipeline_for_schedule
            mock_list.return_value = ([schedule], None)
            mock_update.return_value = "Error: Failed to update timezone"

            response = client.patch(f"{API_BASE}/pipelines/test-pipeline/schedules/test-job/timezone?time_zone=UTC")

            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "error" in response.json()["detail"].lower()


class TestQuartzCronValidationFunction:
    """Tests for validate_quartz_cron function directly."""

    def test_validate_quartz_cron_valid(self):
        """Test valid Quartz cron expressions."""
        from dbrx_api.routes.routes_schedule import validate_quartz_cron

        valid_crons = [
            "0 0 12 * * ?",
            "0 30 9 ? * MON-FRI",
            "0 0 0 1 * ?",
            "0 0 */2 * * ?",
            "0 15 10 ? * * 2024",  # 7 fields with year
        ]

        for cron in valid_crons:
            assert validate_quartz_cron(cron) is True, f"Should be valid: {cron}"

    def test_validate_quartz_cron_invalid_pattern(self):
        """Test cron expressions that fail pattern match."""
        from dbrx_api.routes.routes_schedule import validate_quartz_cron

        invalid_crons = [
            "invalid",
            "* * * * *",  # Only 5 fields
            "",
            "   ",
        ]

        for cron in invalid_crons:
            assert validate_quartz_cron(cron) is False, f"Should be invalid: {cron}"

    def test_validate_quartz_cron_invalid_parts(self):
        """Test cron expressions with invalid characters in parts."""
        from dbrx_api.routes.routes_schedule import validate_quartz_cron

        # These should fail the individual part validation (line 52)
        invalid_crons = [
            "0 0 12 * * @",  # @ not allowed
            "0 0 12 * * $SUN",  # $ not allowed
            "0 0 12 * * !MON",  # ! not allowed
        ]

        for cron in invalid_crons:
            assert validate_quartz_cron(cron) is False, f"Should be invalid: {cron}"
