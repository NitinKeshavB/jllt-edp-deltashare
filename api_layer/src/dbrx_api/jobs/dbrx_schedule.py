"""Module for managing Databricks job schedules and notifications."""

from datetime import datetime
from datetime import timezone as dt_timezone
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import CronSchedule
    from databricks.sdk.service.jobs import JobEmailNotifications
    from databricks.sdk.service.jobs import PauseStatus
    from databricks.sdk.service.jobs import PipelineTask
    from databricks.sdk.service.jobs import Task
    from databricks.sdk.service.pipelines import CreatePipelineResponse
    from databricks.sdk.service.pipelines import GetPipelineResponse
    from databricks.sdk.service.pipelines import PipelineStateInfo
    from databricks.sdk.service.pipelines import UpdateInfo

    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    CreatePipelineResponse = None  # type: ignore[misc, assignment]
    GetPipelineResponse = None  # type: ignore[misc, assignment]
    UpdateInfo = None  # type: ignore[misc, assignment]
    PipelineStateInfo = None  # type: ignore[misc, assignment]

from dbrx_api.dbrx_auth.token_gen import get_auth_token

# list schedules
# get schedule for pipeline
# create schedule for pipeline
# update schedule time
# delete schedule


def list_schedules(
    dltshr_workspace_url: str,
    pipeline_id: Optional[str] = None,
    pipeline_ids: Optional[List[str]] = None,
    max_results: Optional[int] = None,
    page_token: Optional[str] = None,
) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """
    List Databricks job schedules with detailed schedule and notification information.

    This function retrieves jobs and extracts:
    - Cron schedules (time-based)
    - Trigger schedules (event-based: file arrival, pipeline completion, etc.)
    - Continuous job settings
    - Email notifications and notification settings

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_id: Optional single pipeline ID to filter jobs (only jobs containing this pipeline)
        pipeline_ids: Optional list of pipeline IDs to filter jobs (jobs containing any of these pipelines)
        max_results: Maximum number of jobs to return (None = all jobs, default: 100)
        page_token: Optional page token for pagination

    Returns:
        Tuple of (schedules_list, next_page_token):
            - schedules_list: List of dictionaries containing job schedule information:
                - job_id: Job ID
                - job_name: Job name
                - task_types: List of task types (pipeline, notebook, spark_python, sql, etc.)
                - pipeline_ids: List of pipeline IDs if job contains pipeline tasks
                - schedule_type: Type of schedule (cron, trigger, continuous, none)
                - cron_schedule: Cron expression and timezone (if cron-based)
                - trigger_schedule: Trigger details (if trigger-based)
                - continuous_settings: Continuous job settings (if continuous)
                - schedule_status: Schedule status (ACTIVE, PAUSED, NO_SCHEDULE)
                - notifications: Email and webhook notifications
            - next_page_token: Token for next page, None if no more results

    Raises:
        ImportError: If Databricks SDK is not available
        Exception: If authentication or API call fails

    Example:
        >>> # Get all jobs (up to 100 by default)
        >>> jobs, next_token = list_schedules("https://adb-123.azuredatabricks.net/")
        >>>
        >>> # Get specific number of jobs
        >>> jobs, next_token = list_schedules(
        ...     "https://adb-123.azuredatabricks.net/",
        ...     max_results=50
        ... )
        >>>
        >>> # Get next page
        >>> jobs, next_token = list_schedules(
        ...     "https://adb-123.azuredatabricks.net/",
        ...     max_results=50,
        ...     page_token=next_token
        ... )
        >>>
        >>> # Filter by pipeline ID
        >>> pipeline_jobs, next_token = list_schedules(
        ...     "https://adb-123.azuredatabricks.net/",
        ...     pipeline_id="abc123-pipeline-id"
        ... )
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

    # Set default max_results if not provided
    if max_results is None:
        max_results = 100

    # Normalize pipeline filtering - support both single ID and list of IDs
    filter_pipeline_ids = []
    if pipeline_ids:
        filter_pipeline_ids = pipeline_ids
    elif pipeline_id:
        filter_pipeline_ids = [pipeline_id]

    # If filtering by pipeline, fetch more jobs to account for filtering
    # But respect Databricks API limit of 100 per request
    if filter_pipeline_ids:
        fetch_limit = min(max_results * 3, 100)
    else:
        fetch_limit = max_results

    jobs_list = w_client.jobs.list(limit=fetch_limit, page_token=page_token)

    all_jobs = []
    processed_count = 0
    next_page_token = None

    for job_summary in jobs_list:
        # Capture next_page_token if available (check after processing)
        if hasattr(jobs_list, "next_page_token"):
            next_page_token = jobs_list.next_page_token

        # Stop if we have enough results
        if processed_count >= max_results:
            break

        try:
            # Get full job details
            job = w_client.jobs.get(job_id=job_summary.job_id)
            settings = job.settings

            if not settings:
                continue

            job_name = settings.name if settings.name else f"Job-{job_summary.job_id}"

            # Determine task types and pipeline IDs
            task_types = []
            pipeline_ids = []
            if settings.tasks:
                for task in settings.tasks:
                    if task.pipeline_task:
                        task_types.append("pipeline")
                        if task.pipeline_task.pipeline_id:
                            pipeline_ids.append(task.pipeline_task.pipeline_id)
                    elif task.notebook_task:
                        task_types.append("notebook")
                    elif task.spark_python_task:
                        task_types.append("spark_python")
                    elif task.sql_task:
                        task_types.append("sql")
                    elif task.spark_jar_task:
                        task_types.append("spark_jar")
                    elif task.spark_submit_task:
                        task_types.append("spark_submit")
                    elif task.dbt_task:
                        task_types.append("dbt")
                    else:
                        task_types.append("other")

            # Apply pipeline_id filter if specified
            if filter_pipeline_ids:
                # Check if job contains ANY of the specified pipeline IDs
                has_matching_pipeline = any(pid in pipeline_ids for pid in filter_pipeline_ids)
                if not has_matching_pipeline:
                    continue  # Skip jobs that don't contain any of the specified pipelines

            # Extract schedule information
            schedule_type = "none"
            cron_schedule = None
            trigger_schedule = None
            continuous_settings = None
            schedule_status = "NO_SCHEDULE"

            # Check for cron schedule
            if settings.schedule:
                schedule_type = "cron"
                schedule_status = str(settings.schedule.pause_status) if settings.schedule.pause_status else "ACTIVE"
                cron_schedule = {
                    "cron_expression": settings.schedule.quartz_cron_expression,
                    "timezone": settings.schedule.timezone_id,
                    "pause_status": schedule_status,
                }

            # Check for trigger schedule
            elif settings.trigger:
                schedule_type = "trigger"
                schedule_status = str(settings.trigger.pause_status) if settings.trigger.pause_status else "ACTIVE"

                trigger_schedule = {
                    "pause_status": schedule_status,
                }

                # File arrival trigger
                if settings.trigger.file_arrival:
                    trigger_schedule["type"] = "file_arrival"
                    trigger_schedule["url"] = settings.trigger.file_arrival.url
                    trigger_schedule[
                        "min_time_between_triggers_seconds"
                    ] = settings.trigger.file_arrival.min_time_between_triggers_seconds
                    trigger_schedule[
                        "wait_after_last_change_seconds"
                    ] = settings.trigger.file_arrival.wait_after_last_change_seconds

                # Pipeline update trigger
                elif hasattr(settings.trigger, "table_update") and settings.trigger.table_update:
                    trigger_schedule["type"] = "table_update"
                    if hasattr(settings.trigger.table_update, "table_names"):
                        trigger_schedule["table_names"] = list(settings.trigger.table_update.table_names)
                    if hasattr(settings.trigger.table_update, "condition"):
                        trigger_schedule["condition"] = settings.trigger.table_update.condition

                # Periodic trigger
                elif hasattr(settings.trigger, "periodic") and settings.trigger.periodic:
                    trigger_schedule["type"] = "periodic"
                    trigger_schedule["interval"] = settings.trigger.periodic.interval
                    trigger_schedule["unit"] = str(settings.trigger.periodic.unit)

                else:
                    trigger_schedule["type"] = "unknown"

            # Check for continuous job
            elif settings.continuous:
                schedule_type = "continuous"
                schedule_status = (
                    str(settings.continuous.pause_status) if settings.continuous.pause_status else "ACTIVE"
                )
                continuous_settings = {
                    "pause_status": schedule_status,
                }

            # Extract notifications
            notifications = {
                "email_notifications": None,
                "notification_settings": None,
                "webhook_notifications": None,
            }

            # Email notifications (legacy)
            if settings.email_notifications:
                notifications["email_notifications"] = {
                    "on_start": (
                        list(settings.email_notifications.on_start) if settings.email_notifications.on_start else []
                    ),
                    "on_success": (
                        list(settings.email_notifications.on_success)
                        if settings.email_notifications.on_success
                        else []
                    ),
                    "on_failure": (
                        list(settings.email_notifications.on_failure)
                        if settings.email_notifications.on_failure
                        else []
                    ),
                    "on_duration_warning_threshold_exceeded": (
                        list(settings.email_notifications.on_duration_warning_threshold_exceeded)
                        if settings.email_notifications.on_duration_warning_threshold_exceeded
                        else []
                    ),
                    "no_alert_for_skipped_runs": settings.email_notifications.no_alert_for_skipped_runs,
                }

            # Notification settings (new format)
            if hasattr(settings, "notification_settings") and settings.notification_settings:
                notifications["notification_settings"] = {
                    "no_alert_for_skipped_runs": settings.notification_settings.no_alert_for_skipped_runs,
                    "no_alert_for_canceled_runs": settings.notification_settings.no_alert_for_canceled_runs,
                }

            # Webhook notifications
            if hasattr(settings, "webhook_notifications") and settings.webhook_notifications:
                webhook_list = []
                if settings.webhook_notifications.on_start:
                    webhook_list.extend(
                        [{"event": "on_start", "id": w.id} for w in settings.webhook_notifications.on_start]
                    )
                if settings.webhook_notifications.on_success:
                    webhook_list.extend(
                        [{"event": "on_success", "id": w.id} for w in settings.webhook_notifications.on_success]
                    )
                if settings.webhook_notifications.on_failure:
                    webhook_list.extend(
                        [{"event": "on_failure", "id": w.id} for w in settings.webhook_notifications.on_failure]
                    )
                if settings.webhook_notifications.on_duration_warning_threshold_exceeded:
                    webhook_list.extend(
                        [
                            {"event": "on_duration_warning_threshold_exceeded", "id": w.id}
                            for w in settings.webhook_notifications.on_duration_warning_threshold_exceeded
                        ]
                    )
                notifications["webhook_notifications"] = webhook_list

            # Build job info dictionary
            job_info = {
                "job_id": job_summary.job_id,
                "job_name": job_name,
                "task_types": list(set(task_types)),
                "pipeline_ids": pipeline_ids,
                "schedule_type": schedule_type,
                "cron_schedule": cron_schedule,
                "trigger_schedule": trigger_schedule,
                "continuous_settings": continuous_settings,
                "schedule_status": schedule_status,
                "notifications": notifications,
            }
            all_jobs.append(job_info)
            processed_count += 1

        except Exception:
            continue

    return all_jobs, next_page_token


def create_schedule_for_pipeline(
    dltshr_workspace_url: str,
    job_name: str,
    pipeline_id: str,
    cron_expression: str,
    time_zone: str = "UTC",
    paused: bool = False,
    email_notifications: Optional[List[str]] = None,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a scheduled job for a Databricks pipeline.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        job_name: Name for the scheduled job
        pipeline_id: ID of the pipeline to schedule
        cron_expression: Quartz cron expression (6 fields: sec min hour day-of-month month day-of-week)
        time_zone: Timezone for the schedule (e.g., "America/New_York")
        paused: Whether the schedule should be paused initially (default: False)
        email_notifications: List of email addresses for notifications
        tags: Dictionary of tags for the job
        description: Optional description for the scheduled job

    Returns:
        Dictionary containing:
            - Message: Status or error message
            - Status: "success" or "error"
            - Job_name: Job name (on success)
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Error: Databricks SDK is not available"

    try:
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Check if job already exists
        existing_jobs = list(w_client.jobs.list(name=job_name))
        if existing_jobs:
            return f"Job already exists: {job_name}"

        # Build email notifications if provided
        job_email_notifications = None
        if email_notifications:
            job_email_notifications = JobEmailNotifications(
                on_failure=email_notifications,
                on_success=email_notifications,
            )

        print(f"DEBUG: Creating job/schedule {job_name}")
        print(f"DEBUG: - Description being passed: '{description}'")
        print(f"DEBUG: - Pipeline ID: {pipeline_id}")
        print(f"DEBUG: - Cron: {cron_expression}")

        job = w_client.jobs.create(
            name=job_name,
            description=description,
            tasks=[
                Task(
                    task_key="run_pipeline",
                    pipeline_task=PipelineTask(pipeline_id=pipeline_id),
                )
            ],
            schedule=CronSchedule(
                quartz_cron_expression=cron_expression,
                timezone_id=time_zone,
                pause_status=PauseStatus.PAUSED if paused else PauseStatus.UNPAUSED,
            ),
            max_concurrent_runs=1,
            email_notifications=job_email_notifications,
            tags=tags,
        )
        print(f"DEBUG: Job created, job_id: {job.job_id}")
        return f"Schedule created successfully {job_name}"
    except Exception as e:
        return f"Error creating schedule for pipeline {pipeline_id}: {str(e)}"


def delete_schedule_for_pipeline(
    dltshr_workspace_url: str,
    pipeline_id: str,
    job_name: Optional[str] = None,
) -> str:
    """
    Delete scheduled job(s) for a Databricks pipeline.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_id: ID of the pipeline
        job_name: Optional job name to delete. If not provided, deletes all schedules for the pipeline.

    Returns:
        Success or error message string
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Error: Databricks SDK is not available"

    try:
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        if job_name:
            # Delete specific job by name
            jobs = list(w_client.jobs.list(name=job_name))
            if not jobs:
                return f"Job not found: {job_name}"

            job_id = jobs[0].job_id
            w_client.jobs.delete(job_id=job_id)
            return f"Schedule deleted successfully: {job_name} (job_id: {job_id})"

        else:
            # Delete all jobs/schedules for the pipeline
            # Get all schedules for this pipeline
            all_jobs = list(w_client.jobs.list())
            deleted_jobs = []

            for job in all_jobs:
                # Get job details to check if it's for our pipeline
                job_details = w_client.jobs.get(job_id=job.job_id)
                if job_details.settings and job_details.settings.tasks:
                    is_pipeline_job = False
                    for task in job_details.settings.tasks:
                        # Check if this task is a pipeline task for our specific pipeline
                        if (
                            task.pipeline_task is not None
                            and task.pipeline_task.pipeline_id is not None
                            and str(task.pipeline_task.pipeline_id) == str(pipeline_id)
                        ):
                            is_pipeline_job = True
                            break

                    # Only delete if this job is specifically for our pipeline
                    if is_pipeline_job:
                        w_client.jobs.delete(job_id=job.job_id)
                        deleted_jobs.append(job_details.settings.name or str(job.job_id))

            if not deleted_jobs:
                return f"No schedules found for pipeline: {pipeline_id}"

            return f"Deleted {len(deleted_jobs)} schedule(s) for pipeline {pipeline_id}: {', '.join(deleted_jobs)}"

    except Exception as e:
        return f"Error deleting schedule for pipeline {pipeline_id}: {str(e)}"


def update_schedule_for_pipeline(
    dltshr_workspace_url: str,
    job_id: str,
    cron_expression: str,
) -> str:
    """
    Update the cron expression for a scheduled job for a Databricks pipeline.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        job_id: ID of the job to update
        cron_expression: Quartz cron expression (6 fields: sec min hour day-of-month month day-of-week)

    Returns:
        Success or error message string
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Error: Databricks SDK is not available"

    # Clean the cron expression - remove any surrounding quotes
    cron_expression = cron_expression.strip().strip('"').strip("'")

    try:
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get current job settings
        job = w_client.jobs.get(job_id=job_id)
        if not job.settings:
            return f"Error: Job {job_id} has no settings"

        current_settings = job.settings
        current_schedule = current_settings.schedule

        # Preserve current timezone and pause status, or use defaults (UTC)
        current_timezone = current_schedule.timezone_id if current_schedule and current_schedule.timezone_id else "UTC"
        current_pause_status = current_schedule.pause_status if current_schedule else PauseStatus.UNPAUSED

        # Create new schedule with updated cron expression
        new_schedule = CronSchedule(
            quartz_cron_expression=cron_expression,
            timezone_id=current_timezone,
            pause_status=current_pause_status if current_pause_status else PauseStatus.UNPAUSED,
        )

        # Clear other trigger types (only one can be set: schedule, trigger, or continuous)
        current_settings.trigger = None
        current_settings.continuous = None
        current_settings.schedule = new_schedule

        w_client.jobs.reset(job_id=job_id, new_settings=current_settings)
        return f"Schedule updated successfully: {job_id} with cron expression {cron_expression}"
    except Exception as e:
        return f"Error updating schedule for job {job_id} with cron expression {cron_expression}: {str(e)}"


def update_timezone_for_schedule(
    dltshr_workspace_url: str,
    job_id: str,
    time_zone: str = "UTC",
) -> str:
    """
    Update the timezone for a scheduled job for a Databricks pipeline.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        job_id: ID of the job to update
        time_zone: Timezone for the schedule (default: "UTC", e.g., "America/New_York")

    Returns:
        Success or error message string
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Error: Databricks SDK is not available"
    try:
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get current job settings
        job = w_client.jobs.get(job_id=job_id)
        if not job.settings:
            return f"Error: Job {job_id} has no settings"

        current_settings = job.settings
        current_schedule = current_settings.schedule

        if not current_schedule or not current_schedule.quartz_cron_expression:
            return f"Error: Job {job_id} does not have an existing schedule to update"

        # Preserve current cron expression and pause status
        current_cron = current_schedule.quartz_cron_expression
        current_pause_status = current_schedule.pause_status if current_schedule else PauseStatus.UNPAUSED

        # Create new schedule with updated timezone
        new_schedule = CronSchedule(
            quartz_cron_expression=current_cron,
            timezone_id=time_zone,
            pause_status=current_pause_status if current_pause_status else PauseStatus.UNPAUSED,
        )

        # Clear other trigger types (only one can be set: schedule, trigger, or continuous)
        current_settings.trigger = None
        current_settings.continuous = None
        current_settings.schedule = new_schedule

        w_client.jobs.reset(job_id=job_id, new_settings=current_settings)
        return f"Timezone updated successfully: {job_id} with time zone {time_zone}"
    except Exception as e:
        return f"Error updating schedule for job {job_id} with time zone {time_zone}: {str(e)}"


if __name__ == "__main__":
    # Example usage

    val = delete_schedule_for_pipeline(
        dltshr_workspace_url="https://adb-3328600036097005.5.azuredatabricks.net/",
        pipeline_id="a0e5fdcc-fd91-4823-ba73-b859b6462530",
        job_name="test-schedule-citibike-",
    )
    print(val)
