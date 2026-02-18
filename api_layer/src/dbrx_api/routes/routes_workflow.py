"""
Workflow API Routes

REST API endpoints for share pack workflow management.
"""

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

    # 2. Smart strategy detection (skip for DELETE; auto-detect for NEW/UPDATE)
    from dbrx_api.workflow.validators.strategy_detector import detect_optimal_strategy

    user_strategy = config.metadata.strategy
    detection_result = None
    validation_warnings = []

    logger.info(f"User specified strategy: {user_strategy}")

    if user_strategy == "DELETE":
        # DELETE uses name-only lists; no strategy auto-detection
        validation_warnings = []
    else:
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

    # 3. Store in database (with deduplication)
    from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

    db_pool = request.app.state.domain_db_pool
    repo = SharePackRepository(db_pool.pool)

    # Generate unique identifier for deduplication
    # Based on: requestor + business_line + project_name (if available)
    requestor = config.metadata.requestor
    business_line = config.metadata.business_line.strip()
    project_name = (config.metadata.project_name or "default").strip() or "default"

    # Create a stable share pack name for deduplication
    stable_name = f"SharePack_{requestor}_{business_line}_{project_name}".replace(" ", "_")

    # Check if share pack already exists
    existing_share_pack = await repo.get_by_name(stable_name)

    file_format = "yaml" if file.filename.endswith((".yaml", ".yml")) else "xlsx"

    if existing_share_pack:
        # Reuse existing share pack ID
        share_pack_id = existing_share_pack["share_pack_id"]
        share_pack_name = existing_share_pack["share_pack_name"]

        logger.info(f"Reusing existing share pack: {share_pack_id} ({share_pack_name})")
        logger.info("Updating share pack with new configuration...")

        # Update the existing share pack with new config
        # This creates a new version in the history while maintaining the same share_pack_id
        await repo.create_from_config(
            share_pack_id=share_pack_id,
            share_pack_name=share_pack_name,
            requested_by=config.metadata.requestor,
            strategy=config.metadata.strategy,
            config=config.dict(),  # Store updated config as JSONB
            file_format=file_format,
            original_filename=file.filename,
        )

        logger.info(f"Share pack updated: {share_pack_id}")
    else:
        # Create new share pack
        share_pack_id = uuid4()
        share_pack_name = stable_name

        logger.info(f"Creating new share pack: {share_pack_id} ({share_pack_name})")

        await repo.create_from_config(
            share_pack_id=share_pack_id,
            share_pack_name=share_pack_name,
            requested_by=config.metadata.requestor,
            strategy=config.metadata.strategy,
            config=config.dict(),  # Store as JSONB
            file_format=file_format,
            original_filename=file.filename,
        )

        logger.info(f"Share pack created: {share_pack_id}")

    # 3b. Resolve tenant and project from metadata and log to DB (best-effort).
    # Tenants and tenant_regions are reference data (manually maintained / admin-uploaded).
    # We only link share pack to an existing tenant that has at least one tenant_region.
    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_project import ProjectRepository
            from dbrx_api.workflow.db.repository_tenant import TenantRegionRepository
            from dbrx_api.workflow.db.repository_tenant import TenantRepository

            tenant_repo = TenantRepository(db_pool.pool)
            tenant_region_repo = TenantRegionRepository(db_pool.pool)
            project_repo = ProjectRepository(db_pool.pool)

            tenant = await tenant_repo.get_by_name(business_line)
            if not tenant:
                logger.warning(
                    "Skipping tenant/project link: tenant not found (tenant_regions reference data must be loaded first)",
                    business_line=business_line,
                    share_pack_id=str(share_pack_id),
                )
            else:
                regions = await tenant_region_repo.list_by_tenant(UUID(str(tenant["tenant_id"])))
                if not regions:
                    logger.warning(
                        "Skipping tenant/project link: tenant has no tenant_regions (reference data must be loaded first)",
                        business_line=business_line,
                        tenant_id=str(tenant["tenant_id"]),
                        share_pack_id=str(share_pack_id),
                    )
                else:
                    project = await project_repo.get_or_create_by_tenant_and_name(
                        UUID(str(tenant["tenant_id"])),
                        project_name,
                        created_by=requestor,
                    )
                    await repo.update_tenant_and_project(
                        share_pack_id,
                        UUID(str(tenant["tenant_id"])),
                        UUID(str(project["project_id"])),
                        updated_by=requestor,
                    )
                    logger.info(
                        "Resolved tenant and project for share pack",
                        share_pack_id=str(share_pack_id),
                        tenant_id=str(tenant["tenant_id"]),
                        project_id=str(project["project_id"]),
                    )
        except Exception as resolve_err:
            logger.warning(
                "Failed to resolve tenant/project for share pack (upload succeeded)",
                share_pack_id=str(share_pack_id),
                error=str(resolve_err),
            )

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
    queue_message_count = 0
    try:
        queue_message_count = queue_client.get_queue_length()
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
                "QueueMessageCount": queue_message_count,
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "Message": "Workflow system healthy",
            "DatabaseConnected": db_healthy,
            "QueueConnected": queue_healthy,
            "TablesCount": tables_count,
            "QueueMessageCount": queue_message_count,
        },
    )
