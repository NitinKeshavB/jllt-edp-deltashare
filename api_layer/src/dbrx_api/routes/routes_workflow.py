"""
Workflow API Routes

REST API endpoints for share pack workflow management.
"""

from datetime import datetime
from uuid import UUID
from uuid import uuid4

from fastapi import APIRouter
from fastapi import Depends
from fastapi import File
from fastapi import HTTPException
from fastapi import Request
from fastapi import UploadFile
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

from dbrx_api.dependencies import get_settings
from dbrx_api.dependencies import get_workspace_url
from dbrx_api.schemas.schemas_workflow import SharePackStatusResponse
from dbrx_api.schemas.schemas_workflow import SharePackUploadResponse
from dbrx_api.schemas.schemas_workflow import WorkflowHealthResponse
from dbrx_api.settings import Settings

ROUTER_WORKFLOW = APIRouter(tags=["Workflow"], prefix="/workflow")


@ROUTER_WORKFLOW.post(
    "/sharepack/upload_and_validate",
    response_model=SharePackUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload and validate share pack for provisioning",
    responses={
        202: {"description": "Share pack uploaded, validated, and queued for provisioning"},
        400: {"description": "Invalid file format or validation error"},
    },
)
async def upload_and_validate_sharepack(
    request: Request,
    file: UploadFile = File(..., description="YAML or Excel share pack configuration file"),
    workspace_url: str = Depends(get_workspace_url),
    settings: Settings = Depends(get_settings),
):
    """
    Upload and validate a share pack configuration file (YAML or Excel) for provisioning.

    Returns 202 Accepted - processing happens asynchronously via Azure Storage Queue.
    """
    logger.info(f"Received share pack upload: {file.filename}")

    # 1. Parse file
    from dbrx_api.workflow.parsers.parser_factory import parse_sharepack_file

    content = await file.read()

    try:
        config = parse_sharepack_file(content, file.filename)
        logger.debug(f"Parsed share pack: {len(config.recipient)} recipients, {len(config.share)} shares")
    except Exception as e:
        logger.warning(f"Parse error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file format: {str(e)}",
        )

    # 2. Smart strategy detection (auto-detect optimal strategy)
    from dbrx_api.workflow.validators.strategy_detector import detect_optimal_strategy

    user_strategy = config.metadata.strategy
    detection_result = None
    validation_warnings = []

    logger.info(f"User specified strategy: {user_strategy}")

    try:
        # Get token manager from app state (for auth)
        token_manager = getattr(request.app.state, "token_manager", None)

        # Detect optimal strategy based on existing resources
        detection_result = await detect_optimal_strategy(
            workspace_url=workspace_url,
            config=config.dict(),
            user_strategy=user_strategy,
            token_manager=token_manager,
        )

        # Update strategy if auto-corrected
        if detection_result.strategy_changed:
            logger.warning(f"Strategy auto-corrected: {user_strategy} → {detection_result.final_strategy}")
            config.metadata.strategy = detection_result.final_strategy

        validation_warnings = detection_result.warnings

        # Log detection summary
        logger.info(f"Strategy detection: {detection_result.get_summary()}")

    except Exception as e:
        # If detection fails, use user's original strategy
        logger.error(f"Strategy detection failed: {e}", exc_info=True)
        validation_warnings = [f"Could not auto-detect strategy: {str(e)}. Using '{user_strategy}' as specified."]

    # 3. Store in database
    from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

    db_pool = request.app.state.domain_db_pool
    repo = SharePackRepository(db_pool.pool)

    share_pack_id = uuid4()
    share_pack_name = f"SharePack_{config.metadata.requestor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    file_format = "yaml" if file.filename.endswith((".yaml", ".yml")) else "xlsx"

    await repo.create_from_config(
        share_pack_id=share_pack_id,
        share_pack_name=share_pack_name,
        requested_by=config.metadata.requestor,
        strategy=config.metadata.strategy,
        config=config.dict(),  # Store as JSONB
        file_format=file_format,
        original_filename=file.filename,
    )

    logger.info(f"Share pack stored in database: {share_pack_id}")

    # 4. Enqueue for processing
    queue_client = request.app.state.queue_client
    queue_client.enqueue_sharepack(str(share_pack_id), share_pack_name)

    logger.success(f"Share pack enqueued for provisioning: {share_pack_id}")

    # Build response message
    if detection_result and detection_result.strategy_changed:
        message = (
            f"Share pack uploaded and queued. "
            f"Strategy auto-corrected from {user_strategy} to {detection_result.final_strategy} "
            f"based on existing resources."
        )
    else:
        message = "Share pack uploaded successfully and queued for provisioning"

    return SharePackUploadResponse(
        Message=message,
        SharePackId=str(share_pack_id),
        SharePackName=share_pack_name,
        Status="IN_PROGRESS",
        ValidationErrors=[],
        ValidationWarnings=validation_warnings,
    )


@ROUTER_WORKFLOW.get(
    "/sharepack/{share_pack_id}",
    response_model=SharePackStatusResponse,
    summary="Get share pack status",
    responses={
        200: {"description": "Share pack found"},
        404: {"description": "Share pack not found"},
    },
)
async def get_sharepack_status(
    request: Request,
    share_pack_id: str,
):
    """Get current status of a share pack by ID."""
    from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

    db_pool = request.app.state.domain_db_pool
    repo = SharePackRepository(db_pool.pool)

    try:
        share_pack_uuid = UUID(share_pack_id)
        share_pack = await repo.get_current(share_pack_uuid)
    except Exception as e:
        logger.error(f"Error fetching share pack {share_pack_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Share pack not found: {share_pack_id}",
        )

    if not share_pack:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Share pack not found: {share_pack_id}",
        )

    return SharePackStatusResponse(
        SharePackId=str(share_pack["share_pack_id"]),
        SharePackName=share_pack["share_pack_name"],
        Status=share_pack["share_pack_status"],
        Strategy=share_pack["strategy"],
        ProvisioningStatus=share_pack.get("provisioning_status", ""),
        ErrorMessage=share_pack.get("error_message", ""),
        RequestedBy=share_pack["requested_by"],
        CreatedAt=share_pack["effective_from"],
        LastUpdated=share_pack["effective_from"],
    )


@ROUTER_WORKFLOW.get(
    "/health",
    response_model=WorkflowHealthResponse,
    summary="Workflow system health check",
    responses={
        200: {"description": "Workflow system is healthy"},
    },
)
async def workflow_health(
    request: Request,
):
    """
    Check workflow system health.

    Verifies:
    - Database connection
    - Queue connection
    - Database schema (table count)
    """
    db_pool = request.app.state.domain_db_pool
    queue_client = request.app.state.queue_client

    # Check database
    db_healthy = await db_pool.health_check()

    # Check queue
    queue_healthy = False
    try:
        queue_client.get_queue_length()
        queue_healthy = True
    except Exception as e:
        logger.error(f"Queue health check failed: {e}")

    # Get table count
    tables_count = 0
    try:
        counts = await db_pool.get_table_counts()
        tables_count = len(counts)
    except Exception as e:
        logger.error(f"Failed to get table counts: {e}")

    if not db_healthy or not queue_healthy:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "Message": "Workflow system unhealthy",
                "DatabaseConnected": db_healthy,
                "QueueConnected": queue_healthy,
                "TablesCount": tables_count,
            },
        )

    return WorkflowHealthResponse(
        Message="Workflow system healthy",
        DatabaseConnected=db_healthy,
        QueueConnected=queue_healthy,
        TablesCount=tables_count,
    )
