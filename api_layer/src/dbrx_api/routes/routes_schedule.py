"""Schedule API routes for managing Databricks pipeline schedules."""

import re
from typing import Dict
from typing import List
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

from dbrx_api.dependencies import get_workspace_url
from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name as get_pipeline_by_name_sdk
from dbrx_api.jobs.dbrx_pipelines import list_pipelines_with_search_criteria as list_pipelines_with_search_sdk
from dbrx_api.jobs.dbrx_schedule import create_schedule_for_pipeline as create_schedule_for_pipeline_sdk
from dbrx_api.jobs.dbrx_schedule import delete_schedule_for_pipeline as delete_schedule_for_pipeline_sdk
from dbrx_api.jobs.dbrx_schedule import list_schedules as list_schedules_sdk
from dbrx_api.jobs.dbrx_schedule import update_schedule_for_pipeline as update_cron_expression_for_schedule_sdk
from dbrx_api.jobs.dbrx_schedule import update_timezone_for_schedule as update_timezone_for_schedule_sdk

ROUTER_DBRX_SCHEDULE = APIRouter(tags=["Schedule"])

DATABRICKS_JOB_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\s\.]+$")
DATABRICKS_JOB_NAME_MAX_LENGTH = 256

# Quartz cron pattern: 6 required fields + optional 7th (year)
QUARTZ_CRON_PATTERN = re.compile(r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)(\s+\S+)?$")


def validate_quartz_cron(cron_expression: str) -> bool:
    """Validate Quartz cron expression format."""
    cron_expression = cron_expression.strip().strip('"').strip("'")

    if not QUARTZ_CRON_PATTERN.match(cron_expression):
        return False

    parts = cron_expression.split()
    if len(parts) < 6 or len(parts) > 7:
        return False

    for part in parts[:6]:
        if not re.match(r"^[0-9a-zA-Z\*\?\-\/\,LW#]+$", part):
            return False

    return True


class CreateScheduleRequest(BaseModel):
    """Request model for creating a schedule for a pipeline."""

    job_name: str = Field(
        ...,
        description="Name for the scheduled job (alphanumeric, hyphens, underscores, spaces, dots; max 256 chars)",
        min_length=1,
        max_length=DATABRICKS_JOB_NAME_MAX_LENGTH,
    )
    cron_expression: str = Field(
        ...,
        description="Quartz cron expression (6 fields: sec min hour day-of-month month day-of-week)",
        examples=["0 0 12 * * ?", "0 30 9 ? * MON-FRI"],
    )
    time_zone: str = Field(default="UTC", description="Timezone for the schedule (e.g., 'America/New_York', 'UTC')")
    paused: bool = Field(default=False, description="Whether the schedule should be paused initially")
    email_notifications: Optional[List[str]] = Field(
        default=None, description="List of email addresses for notifications"
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dictionary of tags for the job")

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, v: str) -> str:
        """Validate job name follows Databricks naming conventions."""
        v = v.strip()
        if not v:
            raise ValueError("Job name cannot be empty or whitespace only")
        if not DATABRICKS_JOB_NAME_PATTERN.match(v):
            raise ValueError(
                "Job name must contain only alphanumeric characters, hyphens, underscores, spaces, and dots"
            )
        return v

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        """Validate cron expression is valid Quartz format."""
        # Strip quotes that might be passed
        v = v.strip().strip('"').strip("'")
        if not validate_quartz_cron(v):
            raise ValueError(
                "Invalid Quartz cron expression. Must have 6-7 fields "
                "(seconds minutes hours day-of-month month day-of-week [year]). "
                "Example: '0 0 12 * * ?' for daily at noon."
            )
        return v


# =============================================================================
# LIST SCHEDULES ENDPOINTS
# =============================================================================


@ROUTER_DBRX_SCHEDULE.get(
    "/schedules",
    responses={
        status.HTTP_200_OK: {
            "description": "List of all schedules (auto-paginated)",
            "content": {
                "application/json": {
                    "example": {
                        "total": 2,
                        "schedules": [
                            {"job_id": "123", "job_name": "daily-etl"},
                            {"job_id": "456", "job_name": "hourly-sync"},
                        ],
                    }
                }
            },
        },
    },
)
async def list_schedules_all(
    request: Request,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
    page_size: int = Query(
        default=100,
        ge=1,
        le=100,
        description="Number of results to fetch per internal API call (default: 100)",
    ),
    pipeline_name_search_string: Optional[str] = Query(
        default=None, description="Optional search string to filter pipelines by name"
    ),
) -> dict:
    """
    List all schedules across all pipelines.

    This endpoint automatically handles pagination internally and returns all schedules.
    The page_size parameter controls the batch size for internal API calls.

    Args:
        page_size: Number of results per internal API call (default: 100)
        pipeline_name_search_string: Optional search string to filter pipelines by name

    Returns:
        Complete list of all schedules
    """
    logger.info(
        "Listing all schedules (auto-paginated)",
        page_size=page_size,
        pipeline_name_search_string=pipeline_name_search_string,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # If filtering by pipeline name, get pipeline IDs first
    pipeline_ids = None
    if pipeline_name_search_string:
        pipelines = list_pipelines_with_search_sdk(
            dltshr_workspace_url=workspace_url,
            filter_expr=pipeline_name_search_string,
        )
        if pipelines:
            pipeline_ids = [p.pipeline_id for p in pipelines if p.pipeline_id]
        else:
            return {
                "total": 0,
                "schedules": [],
            }

    # Automatically fetch all schedules using pagination
    all_schedules: List[dict] = []
    current_token: Optional[str] = None

    while True:
        schedules, next_token = list_schedules_sdk(
            dltshr_workspace_url=workspace_url,
            max_results=page_size,
            page_token=current_token,
            pipeline_ids=pipeline_ids,
        )
        all_schedules.extend(schedules)

        if not next_token:
            break
        current_token = next_token

        logger.debug(
            "Fetching next page of schedules",
            fetched_so_far=len(all_schedules),
            has_more=True,
        )

    logger.info(
        "Completed fetching all schedules",
        total_schedules=len(all_schedules),
    )

    return {
        "total": len(all_schedules),
        "schedules": all_schedules,
    }


@ROUTER_DBRX_SCHEDULE.get(
    "/schedules/pipeline/{pipeline_name}",
    responses={
        status.HTTP_200_OK: {
            "description": "List of schedules for the pipeline (auto-paginated)",
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
    },
)
async def list_schedules_for_pipeline(
    request: Request,
    response: Response,
    pipeline_name: str,
    workspace_url: str = Depends(get_workspace_url),
    page_size: int = Query(
        default=100,
        ge=1,
        le=100,
        description="Number of results to fetch per internal API call (default: 100)",
    ),
) -> dict:
    """
    List all schedules for a specific pipeline.

    This endpoint automatically handles pagination internally and returns all schedules.
    The page_size parameter controls the batch size for internal API calls.

    Args:
        pipeline_name: Name of the pipeline
        page_size: Number of results per internal API call (default: 100)

    Returns:
        Complete list of all schedules for the pipeline
    """
    logger.info(
        "Listing schedules for pipeline (auto-paginated)",
        pipeline_name=pipeline_name,
        page_size=page_size,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists
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

    # Automatically fetch all schedules using pagination
    all_schedules: List[dict] = []
    current_token: Optional[str] = None

    while True:
        schedules, next_token = list_schedules_sdk(
            dltshr_workspace_url=workspace_url,
            max_results=page_size,
            page_token=current_token,
            pipeline_ids=[pipeline_id],
        )
        all_schedules.extend(schedules)

        if not next_token:
            break
        current_token = next_token

        logger.debug(
            "Fetching next page of schedules for pipeline",
            pipeline_name=pipeline_name,
            fetched_so_far=len(all_schedules),
            has_more=True,
        )

    logger.info(
        "Completed fetching all schedules for pipeline",
        pipeline_name=pipeline_name,
        total_schedules=len(all_schedules),
    )

    return {
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "total": len(all_schedules),
        "schedules": all_schedules,
    }


# =============================================================================
# CREATE SCHEDULE ENDPOINT
# =============================================================================


@ROUTER_DBRX_SCHEDULE.post(
    "/pipelines/{pipeline_name}/schedules",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Schedule created successfully",
            "content": {"application/json": {"example": {"message": "Schedule created successfully"}}},
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Schedule already exists or job name conflict",
            "content": {"application/json": {"example": {"detail": "Schedule already exists: my-scheduled-job"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Failed to create schedule",
            "content": {"application/json": {"example": {"detail": "Failed to create schedule"}}},
        },
    },
)
async def create_schedule_for_pipeline(
    request: Request,
    response: Response,
    pipeline_name: str,
    schedule_request: CreateScheduleRequest,
    workspace_url: str = Depends(get_workspace_url),
) -> JSONResponse:
    """
    Create a scheduled job for a pipeline.

    Args:
        pipeline_name: Name of the pipeline
        schedule_request: Schedule configuration including job name, cron expression, timezone, etc.

    Returns:
        Success message with job details
    """
    logger.info(
        "Creating schedule for pipeline",
        pipeline_name=pipeline_name,
        job_name=schedule_request.job_name,
        cron_expression=schedule_request.cron_expression,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists
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

    # Check if a schedule with the same cron expression already exists for this pipeline
    existing_schedules, _ = list_schedules_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_ids=[pipeline_id],
    )

    # Clean the cron expression for comparison
    cron_clean = schedule_request.cron_expression.strip().strip('"').strip("'")

    for schedule in existing_schedules:
        if schedule.get("cron_schedule", {}).get("cron_expression") == cron_clean:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A schedule with cron expression '{cron_clean}' already exists for pipeline '{pipeline_name}'",
            )

    # Create the schedule
    result = create_schedule_for_pipeline_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        job_name=schedule_request.job_name,
        cron_expression=cron_clean,
        time_zone=schedule_request.time_zone,
        paused=schedule_request.paused,
        email_notifications=schedule_request.email_notifications,
        tags=schedule_request.tags,
    )

    # Handle result (SDK returns string messages)
    if isinstance(result, str):
        if "already exists" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=result,
            )
        if "error" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    # Log schedule to workflow DB for API-created pipelines (best-effort)
    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            schedules_after, _ = list_schedules_sdk(
                dltshr_workspace_url=workspace_url,
                pipeline_ids=[pipeline_id],
            )
            job_id = None
            for s in schedules_after:
                if s.get("job_name") == schedule_request.job_name:
                    job_id = str(s.get("job_id", ""))
                    break
            if job_id:
                from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

                repo = PipelineRepository(request.app.state.domain_db_pool.pool)
                await repo.update_schedule_from_api(
                    pipeline_name=pipeline_name,
                    databricks_job_id=job_id,
                    cron_expression=cron_clean,
                    timezone_str=schedule_request.time_zone,
                    created_by="api",
                )
                logger.info(
                    "Logged schedule to workflow DB", pipeline_name=pipeline_name, job_name=schedule_request.job_name
                )
        except Exception as db_err:
            logger.warning(
                "Failed to log schedule to workflow DB (Databricks create succeeded)",
                pipeline_name=pipeline_name,
                job_name=schedule_request.job_name,
                error=str(db_err),
            )

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "message": "Schedule created successfully",
            "pipeline_name": pipeline_name,
            "pipeline_id": pipeline_id,
            "job_name": schedule_request.job_name,
            "cron_expression": cron_clean,
            "time_zone": schedule_request.time_zone,
            "paused": schedule_request.paused,
        },
    )


# =============================================================================
# UPDATE SCHEDULE ENDPOINTS
# =============================================================================


@ROUTER_DBRX_SCHEDULE.patch(
    "/pipelines/{pipeline_name}/schedules/{job_name}/cron",
    responses={
        status.HTTP_200_OK: {
            "description": "Cron expression updated successfully",
            "content": {"application/json": {"example": {"message": "Cron expression updated successfully"}}},
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline or schedule not found",
            "content": {"application/json": {"example": {"detail": "Schedule not found: my-job"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Failed to update cron expression",
            "content": {"application/json": {"example": {"detail": "Failed to update cron expression"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
            "content": {"application/json": {"example": {"detail": "Permission denied: User is not the owner"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Schedule already has this cron expression",
            "content": {
                "application/json": {"example": {"detail": "Schedule already has cron expression: 0 0 12 * * ?"}}
            },
        },
    },
)
async def update_cron_expression_for_schedule(
    request: Request,
    response: Response,
    pipeline_name: str,
    job_name: str,
    cron_expression: str = Query(..., description="New Quartz cron expression"),
    workspace_url: str = Depends(get_workspace_url),
) -> dict:
    """
    Update the cron expression for a scheduled job.

    Args:
        pipeline_name: Name of the pipeline
        job_name: Name of the scheduled job to update
        cron_expression: New Quartz cron expression (6 fields: sec min hour day-of-month month day-of-week)

    Returns:
        Success message with updated schedule details
    """
    logger.info(
        "Updating cron expression for schedule",
        pipeline_name=pipeline_name,
        job_name=job_name,
        cron_expression=cron_expression,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Clean the cron expression
    cron_expression = cron_expression.strip().strip('"').strip("'")

    # Validate cron expression
    if not validate_quartz_cron(cron_expression):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Quartz cron expression: {cron_expression}",
        )

    # Check if pipeline exists
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

    # Find the schedule/job by name
    schedules, _ = list_schedules_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_ids=[pipeline_id],
    )

    job_id = None
    existing_cron = None
    for schedule in schedules:
        if schedule.get("job_name") == job_name:
            job_id = schedule.get("job_id")
            # Get existing cron expression from schedule
            cron_schedule = schedule.get("cron_schedule", {})
            existing_cron = cron_schedule.get("cron_expression") if cron_schedule else None
            break

    if not job_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule not found: {job_name} for pipeline {pipeline_name}",
        )

    # Check if the new cron expression is the same as the existing one
    if existing_cron and existing_cron == cron_expression:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Schedule '{job_name}' already has cron expression: {cron_expression}",
        )

    # Update the cron expression
    result = update_cron_expression_for_schedule_sdk(
        dltshr_workspace_url=workspace_url,
        job_id=job_id,
        cron_expression=cron_expression,
    )

    # Handle result
    if isinstance(result, str):
        if "permission denied" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        if "error" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    # Log schedule update to workflow DB for API-created pipelines (best-effort)
    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

            repo = PipelineRepository(request.app.state.domain_db_pool.pool)
            await repo.update_schedule_from_api(
                pipeline_name=pipeline_name,
                databricks_job_id=str(job_id),
                cron_expression=cron_expression,
                created_by="api",
            )
            logger.info(
                "Logged schedule cron update to workflow DB",
                pipeline_name=pipeline_name,
                job_name=job_name,
            )
        except Exception as db_err:
            logger.warning(
                "Failed to log schedule update to workflow DB (Databricks update succeeded)",
                pipeline_name=pipeline_name,
                job_name=job_name,
                error=str(db_err),
            )

    return {
        "message": "Cron expression updated successfully",
        "pipeline_name": pipeline_name,
        "job_name": job_name,
        "job_id": job_id,
        "cron_expression": cron_expression,
    }


@ROUTER_DBRX_SCHEDULE.patch(
    "/pipelines/{pipeline_name}/schedules/{job_name}/timezone",
    responses={
        status.HTTP_200_OK: {
            "description": "Timezone updated successfully",
            "content": {"application/json": {"example": {"message": "Timezone updated successfully"}}},
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline or schedule not found",
            "content": {"application/json": {"example": {"detail": "Schedule not found: my-job"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Failed to update timezone",
            "content": {"application/json": {"example": {"detail": "Failed to update timezone"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
            "content": {"application/json": {"example": {"detail": "Permission denied: User is not the owner"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Schedule already has this timezone",
            "content": {"application/json": {"example": {"detail": "Schedule already has timezone: UTC"}}},
        },
    },
)
async def update_timezone_for_schedule(
    request: Request,
    response: Response,
    pipeline_name: str,
    job_name: str,
    time_zone: str = Query(default="UTC", description="New timezone (e.g., 'America/New_York', 'UTC')"),
    workspace_url: str = Depends(get_workspace_url),
) -> dict:
    """
    Update the timezone for a scheduled job.

    Args:
        pipeline_name: Name of the pipeline
        job_name: Name of the scheduled job to update
        time_zone: New timezone for the schedule (default: "UTC")

    Returns:
        Success message with updated schedule details
    """
    logger.info(
        "Updating timezone for schedule",
        pipeline_name=pipeline_name,
        job_name=job_name,
        time_zone=time_zone,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists
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

    # Find the schedule/job by name
    schedules, _ = list_schedules_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_ids=[pipeline_id],
    )

    job_id = None
    existing_timezone = None
    for schedule in schedules:
        if schedule.get("job_name") == job_name:
            job_id = schedule.get("job_id")
            # Get existing timezone from schedule (field is "timezone" in SDK response)
            cron_schedule = schedule.get("cron_schedule", {})
            existing_timezone = cron_schedule.get("timezone") if cron_schedule else None
            break

    if not job_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule not found: {job_name} for pipeline {pipeline_name}",
        )

    # Check if the new timezone is the same as the existing one
    if existing_timezone and existing_timezone == time_zone:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Schedule '{job_name}' already has timezone: {time_zone}",
        )

    # Update the timezone
    result = update_timezone_for_schedule_sdk(
        dltshr_workspace_url=workspace_url,
        job_id=job_id,
        time_zone=time_zone,
    )

    # Handle result
    if isinstance(result, str):
        if "permission denied" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        if "error" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    # Log schedule update to workflow DB for API-created pipelines (best-effort)
    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

            repo = PipelineRepository(request.app.state.domain_db_pool.pool)
            await repo.update_schedule_from_api(
                pipeline_name=pipeline_name,
                databricks_job_id=str(job_id),
                timezone_str=time_zone,
                created_by="api",
            )
            logger.info(
                "Logged schedule timezone update to workflow DB",
                pipeline_name=pipeline_name,
                job_name=job_name,
            )
        except Exception as db_err:
            logger.warning(
                "Failed to log schedule update to workflow DB (Databricks update succeeded)",
                pipeline_name=pipeline_name,
                job_name=job_name,
                error=str(db_err),
            )

    return {
        "message": "Timezone updated successfully",
        "pipeline_name": pipeline_name,
        "job_name": job_name,
        "job_id": job_id,
        "time_zone": time_zone,
    }


# =============================================================================
# DELETE SCHEDULE ENDPOINTS
# =============================================================================


@ROUTER_DBRX_SCHEDULE.delete(
    "/pipelines/{pipeline_name}/schedules/{job_name}",
    responses={
        status.HTTP_200_OK: {
            "description": "Schedule deleted successfully",
            "content": {"application/json": {"example": {"message": "Schedule deleted successfully"}}},
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline or schedule not found",
            "content": {"application/json": {"example": {"detail": "Schedule not found: my-job"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
            "content": {"application/json": {"example": {"detail": "Permission denied: User is not the owner"}}},
        },
    },
)
async def delete_schedule_by_job_name(
    request: Request,
    response: Response,
    pipeline_name: str,
    job_name: str,
    workspace_url: str = Depends(get_workspace_url),
) -> dict:
    """
    Delete a specific schedule for a pipeline by job name.

    Args:
        pipeline_name: Name of the pipeline
        job_name: Name of the scheduled job to delete

    Returns:
        Success message confirming deletion
    """
    logger.info(
        "Deleting schedule for pipeline",
        pipeline_name=pipeline_name,
        job_name=job_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists
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

    # Find the schedule/job by name and validate it's associated with this pipeline
    schedules, _ = list_schedules_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_ids=[pipeline_id],
    )

    job_id = None
    for schedule in schedules:
        if schedule.get("job_name") == job_name:
            job_id = schedule.get("job_id")
            break

    if not job_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule '{job_name}' is not associated with pipeline '{pipeline_name}'",
        )

    # Delete the schedule
    result = delete_schedule_for_pipeline_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        job_name=job_name,
    )

    # Handle result
    if isinstance(result, str):
        if "not found" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )
        if "permission denied" in result.lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )

    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

            repo = PipelineRepository(request.app.state.domain_db_pool.pool)
            await repo.update_schedule_from_api(
                pipeline_name=pipeline_name,
                databricks_job_id="",
                cron_expression="",
                timezone_str="UTC",
                created_by="api",
            )
            logger.info(
                "Cleared schedule in workflow DB after deletion",
                pipeline_name=pipeline_name,
                job_name=job_name,
            )
        except Exception as db_err:
            logger.warning(
                "Failed to clear schedule in workflow DB (Databricks delete succeeded)",
                pipeline_name=pipeline_name,
                job_name=job_name,
                error=str(db_err),
            )

    return {
        "message": f"Schedule '{job_name}' deleted successfully",
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "job_name": job_name,
    }


@ROUTER_DBRX_SCHEDULE.delete(
    "/pipelines/{pipeline_name}/schedules",
    responses={
        status.HTTP_200_OK: {
            "description": "All schedules deleted successfully",
            "content": {"application/json": {"example": {"message": "Deleted 3 schedule(s)"}}},
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
    },
)
async def delete_all_schedules_for_pipeline(
    request: Request,
    response: Response,
    pipeline_name: str,
    workspace_url: str = Depends(get_workspace_url),
) -> dict:
    """
    Delete all schedules for a pipeline.

    Args:
        pipeline_name: Name of the pipeline

    Returns:
        Success message with count of deleted schedules
    """
    logger.info(
        "Deleting all schedules for pipeline",
        pipeline_name=pipeline_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists
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

    # Delete all schedules (passing None for job_name deletes all)
    result = delete_schedule_for_pipeline_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        job_name=None,
    )

    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

            repo = PipelineRepository(request.app.state.domain_db_pool.pool)
            await repo.update_schedule_from_api(
                pipeline_name=pipeline_name,
                databricks_job_id="",
                cron_expression="",
                timezone_str="UTC",
                created_by="api",
            )
            logger.info(
                "Cleared all schedules in workflow DB after deletion",
                pipeline_name=pipeline_name,
            )
        except Exception as db_err:
            logger.warning(
                "Failed to clear schedules in workflow DB (Databricks delete succeeded)",
                pipeline_name=pipeline_name,
                error=str(db_err),
            )

    return {
        "message": result if isinstance(result, str) else "Schedules deleted successfully",
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
    }
