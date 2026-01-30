"""Fixtures for Schedule API testing."""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_schedule_info():
    """Create a mock schedule/job info dictionary."""

    def _create_schedule(
        job_id: str = "123456789",
        job_name: str = "test-schedule-job",
        pipeline_ids: Optional[List[str]] = None,
        cron_expression: str = "0 0 12 * * ?",
        timezone: str = "UTC",
        pause_status: str = "UNPAUSED",
        schedule_type: str = "cron",
    ) -> Dict[str, Any]:
        """Factory function to create schedule info dictionaries."""
        return {
            "job_id": job_id,
            "job_name": job_name,
            "pipeline_ids": pipeline_ids or ["test-pipeline-id-123"],
            "task_types": ["pipeline"],
            "schedule_type": schedule_type,
            "cron_schedule": {
                "cron_expression": cron_expression,
                "timezone": timezone,
                "pause_status": pause_status,
            },
            "trigger_schedule": None,
            "continuous_settings": None,
            "schedule_status": pause_status,
            "notifications": {
                "email_notifications": None,
                "notification_settings": None,
                "webhook_notifications": None,
            },
        }

    return _create_schedule


@pytest.fixture
def sample_schedule_list(mock_schedule_info):
    """Create a sample list of schedules."""
    return [
        mock_schedule_info(job_id="111", job_name="daily-etl-job", cron_expression="0 0 6 * * ?"),
        mock_schedule_info(job_id="222", job_name="hourly-sync-job", cron_expression="0 0 * * * ?"),
        mock_schedule_info(job_id="333", job_name="weekly-report-job", cron_expression="0 0 9 ? * MON"),
    ]


@pytest.fixture
def sample_create_schedule_request():
    """Sample request body for creating a schedule."""
    return {
        "job_name": "new-schedule-job",
        "cron_expression": "0 0 12 * * ?",
        "time_zone": "UTC",
        "paused": False,
        "email_notifications": ["test@example.com"],
        "tags": {"environment": "test"},
    }


@pytest.fixture
def sample_create_schedule_request_minimal():
    """Minimal request body for creating a schedule (only required fields)."""
    return {
        "job_name": "minimal-schedule-job",
        "cron_expression": "0 30 8 * * ?",
    }


@pytest.fixture
def sample_update_schedule_request():
    """Sample request body for updating a schedule."""
    return {
        "cron_expression": "0 0 18 * * ?",
        "time_zone": "America/New_York",
    }


@pytest.fixture
def sample_update_cron_only_request():
    """Sample request body for updating only cron expression."""
    return {
        "cron_expression": "0 0 6 * * ?",
    }


@pytest.fixture
def sample_update_timezone_only_request():
    """Sample request body for updating only timezone."""
    return {
        "time_zone": "Europe/London",
    }


@pytest.fixture
def mock_pipeline_for_schedule():
    """Mock pipeline object for schedule tests."""
    mock = MagicMock()
    mock.pipeline_id = "test-pipeline-id-123"
    mock.name = "test-pipeline"
    return mock


@pytest.fixture
def mock_auth_token():
    """Mock authentication token for schedule tests."""
    from datetime import datetime
    from datetime import timedelta
    from datetime import timezone
    from unittest.mock import patch

    test_token = "test-databricks-token"
    test_expiry = datetime.now(timezone.utc) + timedelta(hours=1)

    with patch("dbrx_api.jobs.dbrx_schedule.get_auth_token") as mock:
        mock.return_value = (test_token, test_expiry)
        yield mock
