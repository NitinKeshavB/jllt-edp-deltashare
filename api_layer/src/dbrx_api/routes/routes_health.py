"""Health check endpoints for monitoring application status."""

from datetime import datetime
from datetime import timezone

from fastapi import APIRouter
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

ROUTER_HEALTH = APIRouter(tags=["Health"])


@ROUTER_HEALTH.get(
    "/health",
    summary="Health check endpoint",
    description="Basic health check that returns application status and metadata",
    responses={
        status.HTTP_200_OK: {
            "description": "Application is healthy",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "timestamp": "2026-01-05T12:00:00.000000Z",
                        "service": "Delta Share API",
                        "version": "v1",
                    }
                }
            },
        }
    },
)
async def health_check(request: Request):
    """
    Basic health check endpoint.

    Returns application status and metadata. This endpoint is lightweight
    and does not perform any external dependency checks.

    Used by:
    - Azure Web App health monitoring
    - Load balancers
    - Kubernetes liveness probes
    """
    settings = request.app.state.settings

    response_data = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "Delta Share API",
        "version": "v1",
        "workspace_url": settings.dltshr_workspace_url,
    }

    logger.debug("Health check requested", status="healthy")

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=response_data,
    )


@ROUTER_HEALTH.post(
    "/health/logging/test",
    summary="Test blob storage logging",
    description="Force a test log upload to verify blob storage is working correctly",
    responses={
        status.HTTP_200_OK: {
            "description": "Test upload completed",
        }
    },
)
async def test_blob_logging(request: Request):
    """
    Test blob storage logging by forcing a test log upload.

    This endpoint creates a test log entry and attempts to upload it to blob storage.
    Use this to verify that:
    - Authentication is working (SAS token, connection string, or managed identity)
    - Container exists or can be created
    - Write permissions are correct
    """
    from dbrx_api.monitoring.logger import _azure_blob_handler

    if not _azure_blob_handler:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "error": "Azure Blob Storage handler not initialized",
                "message": "Blob logging is not enabled or failed to initialize",
            },
        )

    try:
        test_result = _azure_blob_handler.test_upload()

        response_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "test_result": test_result,
        }

        if test_result["success"]:
            logger.info("Blob storage test upload successful", blob_name=test_result["blob_name"])
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=response_data,
            )
        else:
            logger.warning("Blob storage test upload failed", error=test_result.get("error"))
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=response_data,
            )
    except Exception as e:
        import traceback

        logger.error("Error during blob storage test", error=str(e), traceback=traceback.format_exc())
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
