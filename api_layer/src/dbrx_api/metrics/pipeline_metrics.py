"""
Pipeline Metrics Extraction SDK for Databricks DLT Pipelines.

This module provides functions to extract comprehensive pipeline run metrics
from Databricks Delta Live Tables (DLT) pipelines using the Databricks SDK.

Main Functions:
    get_pipeline_metrics: Extract metrics for all DLT pipeline update runs
    get_job_run_metrics: Extract metrics for all job runs associated with a pipeline

Usage Example - Pipeline Metrics:
    from dbrx_api.metrics.pipeline_metrics import get_pipeline_metrics

    result = get_pipeline_metrics(
        dltshr_workspace_url="https://adb-xxx.azuredatabricks.net/",
        pipeline_id="pipeline-id-here",
        start_timestamp="2024-01-01T00:00:00Z"  # Optional
    )

    if isinstance(result, list):
        # Success - result is a list of run dictionaries
        for run in result:
            print(f"Update: {run['update_id']}, Status: {run['run_status']}")
    else:
        # Error - result is a plain message string
        print(result)

Usage Example - Job Run Metrics:
    from dbrx_api.metrics.pipeline_metrics import get_job_run_metrics

    result = get_job_run_metrics(
        dltshr_workspace_url="https://adb-xxx.azuredatabricks.net/",
        pipeline_id="pipeline-id-here",
        start_timestamp="2024-01-01T00:00:00Z"  # Optional
    )

    if isinstance(result, list):
        # Success - result is a list of job run dictionaries
        for run in result:
            print(f"Job: {run['job_name']}, Duration: {run['duration_seconds']}s")
    else:
        # Error - result is a plain message string
        print(result)

Note:
    - Row count metrics are currently set to 0 as they are not available
      through the Databricks SDK or REST API
    - These metrics would need to be queried from the DLT Event Log Delta table
"""

from datetime import datetime
from datetime import timezone as dt_timezone
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

try:
    from databricks.sdk import WorkspaceClient

    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False


from dbrx_api.dbrx_auth.token_gen import get_auth_token
from dbrx_api.jobs.dbrx_schedule import list_schedules


def _format_timestamp(ms_timestamp: Optional[int]) -> Optional[str]:
    """Convert millisecond timestamp to ISO format string."""
    if ms_timestamp is None:
        return None
    dt_obj = datetime.fromtimestamp(ms_timestamp / 1000, tz=dt_timezone.utc)
    return dt_obj.isoformat()


def _calculate_duration_seconds(start_ms: Optional[int], end_ms: Optional[int]) -> Optional[float]:
    """Calculate duration in seconds between start and end timestamps."""
    if start_ms is None or end_ms is None:
        return None
    return (end_ms - start_ms) / 1000.0


def _handle_pipeline_error(error: Exception, operation: str, pipeline_id: str) -> str:
    """
    Handle Databricks SDK errors and return user-friendly messages.

    Args:
        error: The caught exception
        operation: Operation being performed
        pipeline_id: Pipeline ID involved

    Returns:
        User-friendly message
    """
    error_msg = str(error)
    error_lower = error_msg.lower()

    # Permission denied
    if "PERMISSION_DENIED" in error_msg or "permission denied" in error_lower or "not an owner" in error_lower:
        return f"Permission denied for pipeline {pipeline_id}"

    # Resource not found
    if "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_lower or "not found" in error_lower:
        return f"Pipeline not found for pipeline {pipeline_id}"

    # Invalid state
    if "INVALID_STATE" in error_msg or "invalid state" in error_lower:
        return "Pipeline is in an invalid state"

    # Already running
    if "already running" in error_lower or "concurrent" in error_lower:
        return "Pipeline already has an active update running"

    # Authentication errors
    if "authentication" in error_lower or "unauthorized" in error_lower or "token" in error_lower:
        return "Authentication failed"

    # Unexpected error - return generic message
    return f"Failed to get pipeline metrics: {error_msg}"


def get_pipeline_metrics(
    dltshr_workspace_url: str,
    pipeline_id: str,
    start_timestamp: Optional[str] = None,
) -> Union[List[Dict[str, Any]], str]:
    """
    Extract pipeline metrics for ALL pipeline runs (all statuses).

    This function returns ALL pipeline update runs regardless of their status
    (COMPLETED, FAILED, CANCELED, RUNNING, QUEUED, etc.).

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_id: Pipeline ID
        start_timestamp: Optional ISO timestamp (e.g., "2024-01-23T10:30:00Z")
                        - If None: Returns ALL pipeline runs (no time filtering)
                        - If provided: Returns runs from timestamp onwards (inclusive)

    Returns:
        List of dictionaries, one per pipeline run:
        [
            {
                "pipeline_id": str,
                "pipeline_name": str,
                "pipeline_state": str,
                "catalog": str,
                "target_schema": str,
                "update_id": str,
                "run_status": str,  # All statuses: COMPLETED, FAILED, CANCELED, RUNNING, etc.
                "is_full_refresh": bool,
                "triggered_by": str,
                "start_time": str,
                "end_time": str,
                "duration_seconds": float,
                "creation_time": str,
                "tables": [
                    {
                        "table_name": str,
                        "source_table": str,
                        "rows_output": int,
                        "rows_upserted": int,
                        "rows_deleted": int,
                        "bytes_output": int
                    }
                ],
                "total_rows_output": int,
                "total_rows_upserted": int,
                "total_rows_deleted": int,
                "total_rows_failed": int
            }
        ]

        Returns error message string if an error occurs.
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Databricks SDK is not available"

    if not pipeline_id:
        return "Pipeline ID is required"

    if not dltshr_workspace_url:
        return "Workspace URL is required"

    try:
        # Parse timestamp filter if provided
        start_time_filter: Optional[datetime] = None
        if start_timestamp:
            try:
                if start_timestamp.endswith("Z"):
                    start_time_filter = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
                else:
                    start_time_filter = datetime.fromisoformat(start_timestamp).replace(tzinfo=dt_timezone.utc)
            except ValueError:
                return "Invalid timestamp format. Expected ISO format (e.g., '2024-01-23T10:30:00Z')"

        # Connect to workspace
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get pipeline info
        pipeline = w_client.pipelines.get(pipeline_id=pipeline_id)
        pipeline_name = pipeline.name or "Unknown"
        pipeline_state = str(pipeline.state) if pipeline.state else "UNKNOWN"
        catalog = pipeline.spec.catalog if pipeline.spec else None
        target_schema = pipeline.spec.target if pipeline.spec else None

        # Get all updates with pagination - returns ALL statuses
        all_runs: List[Dict[str, Any]] = []
        page_token = None

        while True:
            # Fetch batch of updates (all statuses are returned by the API)
            response = w_client.pipelines.list_updates(pipeline_id=pipeline_id, max_results=100, page_token=page_token)

            if not response.updates:
                break

            # Process each update - include ALL regardless of status
            for update_summary in response.updates:
                if not update_summary.update_id:
                    continue

                # Initialize with data from update_summary (always available)
                update_id = str(update_summary.update_id)
                run_status = str(update_summary.state) if update_summary.state else "UNKNOWN"
                creation_time_ms = getattr(update_summary, "creation_time", None)

                # Apply timestamp filter (inclusive: >= start_time_filter)
                if start_time_filter and creation_time_ms:
                    update_time = datetime.fromtimestamp(creation_time_ms / 1000, tz=dt_timezone.utc)
                    # Skip runs BEFORE the start_time (keep runs >= start_time)
                    if update_time < start_time_filter:
                        continue

                # Initialize default values
                creation_time: Optional[str] = None
                start_time: Optional[str] = None
                end_time: Optional[str] = None
                duration_seconds: Optional[float] = None
                is_full_refresh = False
                triggered_by = "SYSTEM"

                # Format creation time from summary
                if creation_time_ms:
                    dt_obj = datetime.fromtimestamp(creation_time_ms / 1000, tz=dt_timezone.utc)
                    creation_time = dt_obj.isoformat()
                    start_time = creation_time  # Use creation as start time

                # Try to get detailed update info for additional fields
                # If this fails, we still include the run with basic info
                try:
                    update_response = w_client.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)

                    update = update_response.update
                    if update:
                        # Update run_status from detailed info (more accurate)
                        if update.state:
                            run_status = str(update.state)

                        # Update creation_time from detailed info if available
                        if update.creation_time:
                            dt_obj = datetime.fromtimestamp(update.creation_time / 1000, tz=dt_timezone.utc)
                            creation_time = dt_obj.isoformat()
                            start_time = creation_time

                        # Determine update type
                        if update.config and hasattr(update.config, "full_refresh"):
                            is_full_refresh = bool(update.config.full_refresh)

                        # Get triggered by
                        if update.cause:
                            if hasattr(update.cause, "user_action") and update.cause.user_action:
                                user_name = getattr(update.cause.user_action, "user_name", None)
                                triggered_by = f"USER:{user_name}" if user_name else "USER"
                            elif hasattr(update.cause, "scheduled") and update.cause.scheduled:
                                triggered_by = "SCHEDULED"

                except Exception:
                    # If get_update fails, continue with basic info from update_summary
                    # This ensures we don't skip runs that fail to get detailed info
                    pass

                # Try to get end time from events
                try:
                    events = list(
                        w_client.pipelines.list_pipeline_events(
                            pipeline_id=pipeline_id, max_results=100, filter=f"origin.update_id = '{update_id}'"
                        )
                    )

                    for event in events:
                        # Get end time from terminal state events
                        if event.event_type == "update_progress":
                            message = getattr(event, "message", "")
                            if any(state in message for state in ["COMPLETED", "FAILED", "CANCELED"]):
                                event_timestamp = getattr(event, "timestamp", None)
                                if event_timestamp and isinstance(event_timestamp, str):
                                    end_dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
                                    end_time = end_dt.isoformat()

                                    if creation_time_ms:
                                        start_dt = datetime.fromtimestamp(creation_time_ms / 1000, tz=dt_timezone.utc)
                                        duration_seconds = (end_dt - start_dt).total_seconds()
                                    break

                except Exception:
                    # If events API fails, continue without end time
                    pass

                # Metrics placeholders (not available via SDK)
                total_rows_output = 0
                total_rows_upserted = 0
                total_rows_deleted = 0
                total_rows_failed = 0
                tables_metrics: List[Dict[str, Any]] = []

                # Build run info with all required fields
                run_info: Dict[str, Any] = {
                    "pipeline_id": pipeline_id,
                    "pipeline_name": pipeline_name,
                    "pipeline_state": pipeline_state,
                    "catalog": catalog,
                    "target_schema": target_schema,
                    "update_id": update_id,
                    "run_status": run_status,
                    "is_full_refresh": is_full_refresh,
                    "triggered_by": triggered_by,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration_seconds,
                    "creation_time": creation_time,
                    "tables": tables_metrics,
                    "total_rows_output": total_rows_output,
                    "total_rows_upserted": total_rows_upserted,
                    "total_rows_deleted": total_rows_deleted,
                    "total_rows_failed": total_rows_failed,
                }

                all_runs.append(run_info)

            # Check for next page
            if hasattr(response, "next_page_token") and response.next_page_token:
                page_token = response.next_page_token
            else:
                break

        # Check if any runs were found
        if not all_runs:
            if start_timestamp:
                return f"No pipeline runs found from this start time. Pipeline {pipeline_id}"
            else:
                return f"No pipeline runs found for Pipeline {pipeline_id}"

        return all_runs

    except Exception as e:
        return _handle_pipeline_error(e, "get_pipeline_metrics", pipeline_id)


def get_job_run_metrics(
    dltshr_workspace_url: str,
    pipeline_id: str,
    start_timestamp: Optional[str] = None,
) -> Union[List[Dict[str, Any]], str]:
    """
    Extract job run metrics for ALL jobs associated with a DLT pipeline (all statuses).

    This function returns ALL job runs regardless of their status
    (TERMINATED, RUNNING, PENDING, SKIPPED, INTERNAL_ERROR, etc.) and result state
    (SUCCESS, FAILED, TIMEDOUT, CANCELED, etc.).

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_id: Pipeline ID
        start_timestamp: Optional ISO timestamp (e.g., "2024-01-23T10:30:00Z")
                        - If None: Returns ALL job runs (no time filtering)
                        - If provided: Returns runs from timestamp onwards (inclusive)

    Returns:
        List of dictionaries, one per job run:
        [
            {
                "job_name": str,
                "pipeline_name": str,
                "pipeline_id": str,
                "job_id": int,
                "run_id": int,
                "run_by": str,
                "start_time": str,
                "end_time": str,
                "duration_seconds": float,
                "job_schedule": str,
                "rows_processed": int,
                "run_status": str,  # All statuses: TERMINATED, RUNNING, PENDING, etc.
                "result_state": str  # All states: SUCCESS, FAILED, TIMEDOUT, CANCELED, etc.
            }
        ]

        Returns message string if an error occurs.
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Databricks SDK is not available"

    if not pipeline_id:
        return "Pipeline ID is required"

    if not dltshr_workspace_url:
        return "Workspace URL is required"

    try:
        # Parse timestamp filter if provided
        start_time_filter: Optional[datetime] = None
        if start_timestamp:
            try:
                if start_timestamp.endswith("Z"):
                    start_time_filter = datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
                else:
                    start_time_filter = datetime.fromisoformat(start_timestamp).replace(tzinfo=dt_timezone.utc)
            except ValueError:
                return "Invalid timestamp format. Expected ISO format (e.g., '2024-01-23T10:30:00Z')"

        # Connect to workspace
        session_token = get_auth_token(datetime.now(dt_timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get pipeline info
        try:
            pipeline = w_client.pipelines.get(pipeline_id=pipeline_id)
            pipeline_name = pipeline.name or "Unknown"
        except Exception:
            return f"Pipeline not found - {pipeline_id}"

        # Find all jobs associated with this pipeline using list_schedules
        try:
            pipeline_jobs, _ = list_schedules(
                dltshr_workspace_url=dltshr_workspace_url,
                pipeline_id=pipeline_id,
                max_results=None,  # Get all jobs for this pipeline
            )
        except Exception:
            return f"Failed to fetch jobs for pipeline - {pipeline_id}"

        if not pipeline_jobs:
            return f"No jobs found for this pipeline - {pipeline_id}"

        # Collect all job run metrics (all statuses)
        all_runs: List[Dict[str, Any]] = []
        debug_info: List[str] = []

        for job_info in pipeline_jobs:
            job_id = job_info["job_id"]
            job_name = job_info["job_name"]

            # Get job schedule info from the job_info dict
            schedule_type = job_info.get("schedule_type", "none")
            job_schedule = "Manual"

            if schedule_type == "cron" and job_info.get("cron_schedule"):
                cron = job_info["cron_schedule"]
                job_schedule = f"Cron: {cron.get('cron_expression', 'unknown')}"
            elif schedule_type == "trigger":
                job_schedule = "Trigger-based"
            elif schedule_type == "continuous":
                job_schedule = "Continuous"

            # Get run history for this job - ALL runs regardless of status
            # Note: Databricks API limit is max 25 per page, SDK handles pagination
            try:
                runs_list = list(w_client.jobs.list_runs(job_id=int(job_id), limit=25))
                debug_info.append(f"{job_name}(id={job_id}): {len(runs_list)} runs")
            except Exception as e:
                debug_info.append(f"{job_name}(id={job_id}): ERROR - {str(e)}")
                continue

            runs_processed = 0
            for run in runs_list:
                # Extract start_time for filtering (handle missing start_time)
                run_start_time_ms = getattr(run, "start_time", None)

                # Apply timestamp filter (inclusive: >= start_time_filter)
                if start_time_filter and run_start_time_ms:
                    run_time = datetime.fromtimestamp(run_start_time_ms / 1000, tz=dt_timezone.utc)
                    # Skip runs BEFORE the start_time (keep runs >= start_time)
                    if run_time < start_time_filter:
                        continue

                # Determine who ran the job
                run_by = "SYSTEM"
                if hasattr(run, "creator_user_name") and run.creator_user_name:
                    run_by = f"USER:{run.creator_user_name}"
                elif hasattr(run, "trigger") and run.trigger:
                    trigger_str = str(run.trigger)
                    if "PERIODIC" in trigger_str or "CRON" in trigger_str:
                        run_by = "SCHEDULED"
                    elif "ONE_TIME" in trigger_str:
                        run_by = "MANUAL"

                # Extract run status - ALL statuses included
                run_status = "UNKNOWN"
                if hasattr(run, "state") and run.state:
                    if hasattr(run.state, "life_cycle_state") and run.state.life_cycle_state:
                        run_status = str(run.state.life_cycle_state)

                # Extract result state - ALL result states included
                result_state: Optional[str] = None
                if hasattr(run, "state") and run.state:
                    if hasattr(run.state, "result_state") and run.state.result_state:
                        result_state = str(run.state.result_state)

                # Extract timestamps and duration
                start_time = _format_timestamp(run_start_time_ms)
                end_time = _format_timestamp(getattr(run, "end_time", None))
                duration_seconds = _calculate_duration_seconds(run_start_time_ms, getattr(run, "end_time", None))

                # Build run info - include ALL runs regardless of status
                run_info: Dict[str, Any] = {
                    "job_name": job_name,
                    "pipeline_name": pipeline_name,
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "run_id": getattr(run, "run_id", None),
                    "run_by": run_by,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration_seconds,
                    "job_schedule": job_schedule,
                    "rows_processed": 0,
                    "run_status": run_status,
                    "result_state": result_state,
                }

                all_runs.append(run_info)
                runs_processed += 1

            # Update debug info with processed count
            debug_info[-1] = f"{job_name}(id={job_id}): {len(runs_list)} runs, {runs_processed} processed"

        # Check if any runs were found
        if not all_runs:
            debug_str = "; ".join(debug_info)
            if start_timestamp:
                return f"No job runs found from this start time for pipeline - {pipeline_id}. " f"Debug: [{debug_str}]"
            else:
                return f"No job runs found for pipeline - {pipeline_id}. " f"Debug: [{debug_str}]"

        return all_runs

    except Exception as e:
        return _handle_pipeline_error(e, "get_job_run_metrics", pipeline_id)
