"""Routes for Unity Catalog management."""

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

from dbrx_api.dependencies import get_workspace_url
from dbrx_api.jobs.dbrx_catalog import create_catalog as create_catalog_sdk
from dbrx_api.jobs.dbrx_catalog import get_catalog as get_catalog_sdk
from dbrx_api.jobs.dbrx_catalog import list_catalogs as list_catalogs_sdk
from dbrx_api.schemas.schemas import CreateCatalogRequest

ROUTER_CATALOG = APIRouter(tags=["Catalogs"])


@ROUTER_CATALOG.post(
    "/catalogs/{catalog_name}",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Catalog created successfully",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Catalog 'my_catalog' created successfully and privileges granted to service principal",
                        "catalog_name": "my_catalog",
                        "created": True,
                    }
                }
            },
        },
        status.HTTP_409_CONFLICT: {
            "description": "Catalog already exists",
            "content": {"application/json": {"example": {"detail": "Catalog 'my_catalog' already exists"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Catalog creation failed",
            "content": {
                "application/json": {
                    "examples": {
                        "no_warehouse": {
                            "summary": "No SQL warehouse available",
                            "value": {
                                "detail": "Cannot create catalog 'my_catalog': No SQL warehouse available. Please create a SQL warehouse first."
                            },
                        },
                        "permission_denied": {
                            "summary": "Permission denied",
                            "value": {
                                "detail": "Permission denied: Service principal does not have permission to create catalog 'my_catalog'. Please ensure the service principal has 'CREATE CATALOG' privilege."
                            },
                        },
                    }
                }
            },
        },
    },
)
async def create_catalog(
    request: Request,
    response: Response,
    catalog_name: str = Path(..., min_length=1, description="Name of the catalog (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
    create_request: CreateCatalogRequest = None,
) -> JSONResponse:
    """
    Create a new Unity Catalog.

    This endpoint creates a new Unity Catalog in Databricks and grants privileges to the service principal.

    **What it does:**
    - Creates a catalog using Unity Catalog's default storage (if enabled)
    - Grants ALL PRIVILEGES to the configured service principal
    - Validates catalog name format

    **Requirements:**
    - SQL Warehouse must be available in the workspace
    - Service principal must have CREATE CATALOG privilege
    - Unity Catalog must be enabled with default storage configured

    **Note:**
    This endpoint should be called BEFORE creating pipelines that reference this catalog.
    """
    # Validate catalog name (no leading/trailing spaces)
    if catalog_name != catalog_name.strip():
        logger.warning(
            "Catalog name has leading or trailing spaces",
            catalog_name=catalog_name,
            stripped=catalog_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Catalog name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not catalog_name.strip():
        logger.warning("Catalog name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Catalog name cannot contain only whitespace",
        )

    comment = "Catalog created via Delta Share API"
    external_location = None

    if create_request:
        if create_request.comment:
            comment = create_request.comment
        if create_request.external_location:
            external_location = create_request.external_location

    logger.info(
        "Creating catalog",
        catalog_name=catalog_name,
        comment=comment,
        external_location=external_location,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Create the catalog
    result = create_catalog_sdk(
        workspace_url=workspace_url,
        catalog_name=catalog_name,
        comment=comment,
        external_location=external_location,
    )

    # Handle result
    if isinstance(result, dict):
        if result["success"]:
            response.status_code = status.HTTP_201_CREATED
            logger.info(
                "Catalog created successfully",
                catalog_name=catalog_name,
                created=result["created"],
            )
            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content={
                    "message": result["message"],
                    "catalog_name": catalog_name,
                    "created": result["created"],
                },
            )
        else:
            # Determine appropriate status code based on error message
            error_msg_lower = result["message"].lower()
            if "already exists" in error_msg_lower:
                status_code = status.HTTP_409_CONFLICT
            elif "permission" in error_msg_lower or "forbidden" in error_msg_lower:
                status_code = status.HTTP_403_FORBIDDEN
            else:
                status_code = status.HTTP_400_BAD_REQUEST

            logger.error("Failed to create catalog", catalog_name=catalog_name, error=result["message"])
            raise HTTPException(status_code=status_code, detail=result["message"])
    else:
        logger.error("Unexpected result from create_catalog_sdk", result=result)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error creating catalog",
        )


@ROUTER_CATALOG.get(
    "/catalogs/{catalog_name}",
    responses={
        status.HTTP_200_OK: {
            "description": "Catalog found",
            "content": {
                "application/json": {
                    "example": {
                        "catalog_name": "my_catalog",
                        "exists": True,
                        "owner": "service_principal_id",
                    }
                }
            },
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Catalog not found",
            "content": {"application/json": {"example": {"detail": "Catalog 'my_catalog' does not exist"}}},
        },
    },
)
async def get_catalog(
    request: Request,
    catalog_name: str = Path(..., min_length=1, description="Name of the catalog"),
    workspace_url: str = Depends(get_workspace_url),
) -> JSONResponse:
    """
    Get catalog details.

    Checks if a catalog exists and returns its information.
    """
    logger.info(
        "Getting catalog",
        catalog_name=catalog_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    result = get_catalog_sdk(workspace_url=workspace_url, catalog_name=catalog_name)

    if isinstance(result, dict):
        if result["exists"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "catalog_name": catalog_name,
                    "exists": True,
                    "owner": result.get("owner"),
                },
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_name}' does not exist",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error checking catalog",
        )


@ROUTER_CATALOG.get(
    "/catalogs",
    responses={
        status.HTTP_200_OK: {
            "description": "Catalogs retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "catalogs": [
                            {"name": "catalog1", "owner": "user1"},
                            {"name": "catalog2", "owner": "user2"},
                        ],
                        "count": 2,
                    }
                }
            },
        }
    },
)
async def list_catalogs(
    request: Request,
    workspace_url: str = Depends(get_workspace_url),
) -> JSONResponse:
    """
    List all catalogs in the workspace.

    Returns a list of all Unity Catalogs accessible to the service principal.
    """
    logger.info(
        "Listing catalogs",
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    catalogs = list_catalogs_sdk(workspace_url=workspace_url)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "catalogs": catalogs,
            "count": len(catalogs),
        },
    )
