"""Metrics API routes for extracting Databricks pipeline and job run metrics."""

from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi import status
from loguru import logger

from dbrx_api.dependencies import get_workspace_url
from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name as get_pipeline_by_name_sdk
from dbrx_api.metrics.pipeline_metrics import get_job_run_metrics as get_job_run_metrics_sdk
from dbrx_api.metrics.pipeline_metrics import get_pipeline_metrics as get_pipeline_metrics_sdk

ROUTER_DBRX_METRICS = APIRouter(tags=["Metrics"])


# =============================================================================
# PIPELINE RUN METRICS ENDPOINT
# =============================================================================


@ROUTER_DBRX_METRICS.get(
    "/pipelines/{pipeline_name}/metrics",
    responses={
        status.HTTP_200_OK: {
            "description": "Pipeline run metrics retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "pipeline_name": "my-pipeline",
                        "pipeline_id": "abc123",
                        "total_runs": 5,
                        "runs": [
                            {
                                "pipeline_id": "abc123",
                                "pipeline_name": "my-pipeline",
                                "update_id": "12345",
                                "run_status": "COMPLETED",
                                "start_time": "2024-01-23T10:30:00Z",
                                "end_time": "2024-01-23T10:45:00Z",
                                "duration_seconds": 900,
                            }
                        ],
                    }
                }
            },
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found or no runs found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid request parameters",
            "content": {"application/json": {"example": {"detail": "Invalid timestamp format. Expected ISO format"}}},
        },
    },
)
async def get_pipeline_run_metrics(
    request: Request,
    response: Response,
    pipeline_name: str,
    workspace_url: str = Depends(get_workspace_url),
    start_timestamp: Optional[str] = Query(
        default=None,
        description="Optional ISO timestamp (e.g., '2024-01-23T10:30:00Z'). "
        "If provided, returns runs from this timestamp onwards. "
        "If not provided, returns all runs.",
        examples=["2024-01-23T10:30:00Z", "2024-01-23T10:30:00+00:00"],
    ),
) -> dict:
    """
    Extract pipeline run metrics for a specific pipeline.

    Retrieves comprehensive metrics for all DLT pipeline update runs, including:
    - Pipeline information (ID, name, state, catalog, schema)
    - Run details (update_id, status, full refresh flag, triggered by)
    - Timing information (start time, end time, duration)
    - Table metrics (currently set to 0 as not available via API)
    - Error messages for failed runs

    Args:
        pipeline_name: Name of the pipeline
        start_timestamp: Optional ISO timestamp to filter runs (e.g., '2024-01-23T10:30:00Z')
                        If None, returns ALL pipeline runs
                        If provided, returns runs from timestamp onwards

    Returns:
        Dictionary containing pipeline information and list of run metrics
    """
    logger.info(
        "Extracting pipeline run metrics",
        pipeline_name=pipeline_name,
        start_timestamp=start_timestamp,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists and get pipeline_id
    pipeline = get_pipeline_by_name_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
    )
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline not found: {pipeline_name}",
        )

    pipeline_id = pipeline.pipeline_id

    # Extract pipeline metrics using SDK
    result = get_pipeline_metrics_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        start_timestamp=start_timestamp,
    )

    # Handle SDK response
    if isinstance(result, str):
        # SDK returned an error or no-results message
        result_lower = result.lower()

        # Invalid timestamp format
        if "invalid timestamp" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

        # Pipeline not found (shouldn't happen since we checked, but handle it)
        if "pipeline not found" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )

        # No runs found
        if "no pipeline runs found" in result_lower:
            logger.info(
                "No pipeline runs found",
                pipeline_name=pipeline_name,
                pipeline_id=pipeline_id,
                start_timestamp=start_timestamp,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )

        # Permission denied
        if "permission denied" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )

        # Authentication failed
        if "authentication failed" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=result,
            )

        # Other errors
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result,
        )

    # Success - result is a list of run metrics
    logger.info(
        "Successfully extracted pipeline run metrics",
        pipeline_name=pipeline_name,
        pipeline_id=pipeline_id,
        total_runs=len(result),
    )

    return {
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "total_runs": len(result),
        "runs": result,
    }


# =============================================================================
# JOB RUN METRICS ENDPOINT
# =============================================================================


@ROUTER_DBRX_METRICS.get(
    "/pipelines/{pipeline_name}/job-runs/metrics",
    responses={
        status.HTTP_200_OK: {
            "description": "Job run metrics retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "pipeline_name": "my-pipeline",
                        "pipeline_id": "abc123",
                        "total_runs": 3,
                        "runs": [
                            {
                                "job_name": "my-scheduled-job",
                                "pipeline_name": "my-pipeline",
                                "pipeline_id": "abc123",
                                "job_id": 12345,
                                "run_id": 67890,
                                "run_by": "USER:user@example.com",
                                "start_time": "2024-01-23T10:30:00Z",
                                "end_time": "2024-01-23T10:45:00Z",
                                "duration_seconds": 900,
                                "job_schedule": "Cron: 0 0 12 * * ?",
                                "run_status": "TERMINATED",
                                "result_state": "SUCCESS",
                                "error_message": None,
                            },
                            {
                                "job_name": "my-scheduled-job",
                                "pipeline_name": "my-pipeline",
                                "pipeline_id": "abc123",
                                "job_id": 12345,
                                "run_id": 67891,
                                "run_by": "SCHEDULED",
                                "start_time": "2024-01-23T11:30:00Z",
                                "end_time": "2024-01-23T11:35:00Z",
                                "duration_seconds": 300,
                                "job_schedule": "Cron: 0 0 12 * * ?",
                                "run_status": "TERMINATED",
                                "result_state": "FAILED",
                                "error_message": "Run failed: Pipeline update failed with error...",
                            },
                        ],
                    }
                }
            },
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found, no jobs found, or no runs found",
            "content": {"application/json": {"example": {"detail": "No jobs found for this pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid request parameters",
            "content": {"application/json": {"example": {"detail": "Invalid timestamp format. Expected ISO format"}}},
        },
    },
)
async def get_job_run_metrics(
    request: Request,
    response: Response,
    pipeline_name: str,
    workspace_url: str = Depends(get_workspace_url),
    start_timestamp: Optional[str] = Query(
        default=None,
        description="Optional ISO timestamp (e.g., '2024-01-23T10:30:00Z'). "
        "If provided, returns runs from this timestamp onwards. "
        "If not provided, returns all runs.",
        examples=["2024-01-23T10:30:00Z", "2024-01-23T10:30:00+00:00"],
    ),
) -> dict:
    """
    Extract job run metrics for all jobs associated with a pipeline.

    Retrieves comprehensive metrics for all job runs that trigger the specified pipeline:
    - Job information (job_id, job_name, schedule type)
    - Run details (run_id, run_by, status, result)
    - Timing information (start time, end time, duration)
    - Schedule information (cron expression or schedule type)
    - Row processing metrics (currently set to 0 as not available via API)

    Args:
        pipeline_name: Name of the pipeline
        start_timestamp: Optional ISO timestamp to filter runs (e.g., '2024-01-23T10:30:00Z')
                        If None, returns ALL job runs
                        If provided, returns runs from timestamp onwards

    Returns:
        Dictionary containing pipeline information and list of job run metrics
    """
    logger.info(
        "Extracting job run metrics",
        pipeline_name=pipeline_name,
        start_timestamp=start_timestamp,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists and get pipeline_id
    pipeline = get_pipeline_by_name_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
    )
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline not found: {pipeline_name}",
        )

    pipeline_id = pipeline.pipeline_id

    # Extract job run metrics using SDK
    result = get_job_run_metrics_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        start_timestamp=start_timestamp,
    )

    # Handle SDK response
    if isinstance(result, str):
        # SDK returned an error or no-results message
        result_lower = result.lower()

        # Invalid timestamp format
        if "invalid timestamp" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

        # Pipeline not found (shouldn't happen since we checked, but handle it)
        if "pipeline not found" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )

        # No jobs found for pipeline
        if "no jobs found" in result_lower:
            logger.info(
                "No jobs found for pipeline",
                pipeline_name=pipeline_name,
                pipeline_id=pipeline_id,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )

        # No job runs found
        if "no job runs found" in result_lower:
            logger.info(
                "No job runs found",
                pipeline_name=pipeline_name,
                pipeline_id=pipeline_id,
                start_timestamp=start_timestamp,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )

        # Failed to fetch jobs
        if "failed to fetch jobs" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

        # Permission denied
        if "permission denied" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )

        # Authentication failed
        if "authentication failed" in result_lower:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=result,
            )

        # Other errors
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result,
        )

    # Success - result is a list of job run metrics
    logger.info(
        "Successfully extracted job run metrics",
        pipeline_name=pipeline_name,
        pipeline_id=pipeline_id,
        total_runs=len(result),
    )

    return {
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "total_runs": len(result),
        "runs": result,
    }
