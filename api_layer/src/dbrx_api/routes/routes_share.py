import re
from typing import Optional

from databricks.sdk.service.sharing import ShareInfo
from databricks.sdk.service.sharing import UpdateSharePermissionsResponse
from fastapi import APIRouter
from fastapi import Body
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

from dbrx_api.dependencies import get_workspace_url
from dbrx_api.dltshr.share import add_data_object_to_share
from dbrx_api.dltshr.share import add_recipients_to_share as adding_recipients_to_share
from dbrx_api.dltshr.share import create_share as create_share_func
from dbrx_api.dltshr.share import delete_share
from dbrx_api.dltshr.share import get_shares
from dbrx_api.dltshr.share import list_shares_all
from dbrx_api.dltshr.share import remove_recipients_from_share as removing_recipients_from_share
from dbrx_api.dltshr.share import revoke_data_object_from_share
from dbrx_api.schemas.schemas import AddDataObjectsRequest
from dbrx_api.schemas.schemas import GetSharesQueryParams
from dbrx_api.schemas.schemas import GetSharesResponse

ROUTER_SHARE = APIRouter(tags=["Shares"])


async def _sync_share_to_db(request: Request, share_name: str, workspace_url: str) -> None:
    """Best-effort: re-read share from Databricks and sync current state to workflow DB."""
    if not (hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None):
        return
    try:
        from dbrx_api.dltshr.share import get_share_objects
        from dbrx_api.dltshr.share import get_share_recipients
        from dbrx_api.workflow.db.repository_share import ShareRepository

        repo = ShareRepository(request.app.state.domain_db_pool.pool)
        share_info = get_shares(share_name, workspace_url)
        if not share_info:
            return
        databricks_share_id = str(getattr(share_info, "id", share_name) or share_name)
        desc = ""
        if hasattr(share_info, "comment") and share_info.comment:
            desc = share_info.comment.strip()
        objects = get_share_objects(share_name=share_name, dltshr_workspace_url=workspace_url)
        actual_assets = objects.get("tables", []) + objects.get("views", []) + objects.get("schemas", [])
        actual_recipients = get_share_recipients(share_name=share_name, dltshr_workspace_url=workspace_url)
        await repo.create_or_upsert_from_api(
            share_name=share_name,
            databricks_share_id=databricks_share_id,
            share_assets=actual_assets,
            recipients_attached=actual_recipients,
            description=desc,
            created_by="api",
        )
        logger.info("Synced share state to workflow DB after API update", share_name=share_name)
    except Exception as db_err:
        logger.warning(
            "Best-effort DB sync failed for share (Databricks op succeeded)",
            share_name=share_name,
            error=str(db_err),
        )


@ROUTER_SHARE.get(
    "/shares/{share_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Share not found",
            "content": {"application/json": {"example": {"detail": "Share not found"}}},
        },
    },
)
async def get_shares_by_name(
    request: Request,
    share_name: str,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> ShareInfo:
    """Retrieve detailed information for a specific Delta Sharing share by name."""
    logger.info("Getting share by name", share_name=share_name, workspace_url=workspace_url)
    share = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)

    if share is None:
        logger.warning(
            "Share not found",
            share_name=share_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Share not found: {share_name}",
        )
    else:
        response.status_code = status.HTTP_200_OK
        logger.info("Share retrieved successfully", share_name=share_name, owner=share.owner)
    return share


@ROUTER_SHARE.get(
    "/shares",
    responses={
        status.HTTP_200_OK: {
            "description": "Shares fetched successfully",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "Fetched 5 shares!",
                        "Share": [],
                    }
                }
            },
        },
        status.HTTP_204_NO_CONTENT: {
            "description": "No shares found for search criteria",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "No shares found for search criteria.",
                    }
                }
            },
        },
    },
)
async def list_shares_all_or_with_prefix(
    request: Request,
    response: Response,
    query_params: GetSharesQueryParams = Depends(),
    workspace_url: str = Depends(get_workspace_url),
):
    """List all Delta Sharing shares with optional prefix filtering and pagination."""
    logger.info(
        "Listing shares",
        prefix=query_params.prefix,
        page_size=query_params.page_size,
        workspace_url=workspace_url,
    )
    shares = list_shares_all(
        prefix=query_params.prefix,
        max_results=query_params.page_size,
        dltshr_workspace_url=workspace_url,
    )

    if len(shares) == 0:
        logger.info("No shares found", prefix=query_params.prefix)
        return JSONResponse(
            status_code=status.HTTP_204_NO_CONTENT, content={"detail": "No shares found for search criteria."}
        )

    response.status_code = status.HTTP_200_OK
    message = f"Fetched {len(shares)} shares!"
    logger.info("Shares retrieved successfully", count=len(shares), prefix=query_params.prefix)
    return GetSharesResponse(Message=message, Share=shares)


@ROUTER_SHARE.delete(
    "/shares/{share_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Share not found",
            "content": {"application/json": {"example": {"detail": "Share not found"}}},
        },
        status.HTTP_200_OK: {
            "description": "Deleted Share successfully!",
            "content": {"application/json": {"example": {"detail": "Deleted Share successfully!"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to delete share as user is not the owner",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to delete share as user is not the owner"}
                }
            },
        },
    },
)
async def delete_share_by_name(
    request: Request,
    share_name: str,
    workspace_url: str = Depends(get_workspace_url),
):
    """Permanently delete a Delta Sharing share and all its associated permissions."""
    logger.info(
        "Deleting share",
        share_name=share_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    share = get_shares(share_name, workspace_url)
    if share:
        res = delete_share(share_name=share_name, dltshr_workspace_url=workspace_url)
        if isinstance(res, str) and ("User is not an owner of Share" in res):
            logger.warning(
                "Permission denied to delete share",
                share_name=share_name,
                error=res,
                http_status=403,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to delete share as user is not the owner: {share_name}",
            )
        elif isinstance(res, str) and "not found" in res:
            logger.warning(
                "Share not found for deletion",
                share_name=share_name,
                error=res,
                http_status=404,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Share not found: {share_name}",
            )
        else:
            logger.info("Share deleted successfully", share_name=share_name, status_code=status.HTTP_200_OK)
            if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
                try:
                    from dbrx_api.workflow.db.repository_share import ShareRepository

                    repo = ShareRepository(request.app.state.domain_db_pool.pool)
                    records = await repo.list_by_share_name(share_name)
                    for rec in records:
                        await repo.soft_delete(
                            rec["share_id"],
                            deleted_by="api",
                            deletion_reason="Deleted via API (delete share by name)",
                            request_source="api",
                        )
                    if records:
                        logger.info(
                            "Soft-deleted share records in data model",
                            share_name=share_name,
                            count=len(records),
                        )
                except Exception as db_err:
                    logger.warning(
                        "Failed to soft-delete share in data model (Databricks delete succeeded)",
                        share_name=share_name,
                        error=str(db_err),
                    )
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"message": "Deleted Share successfully!"},
            )
    logger.warning(
        "Share not found for deletion",
        share_name=share_name,
        http_status=404,
        http_method=request.method,
        url_path=str(request.url.path),
    )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Share not found: {share_name}",
    )


@ROUTER_SHARE.post(
    "/shares/{share_name:path}",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Shares created successfully",
            "content": {"application/json": {"example": {"Message": "Share created successfully!"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Share already exists",
            "content": {"application/json": {"example": {"Message": "Share already exists"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid share name",
            "content": {"application/json": {"example": {"Message": "Invalid share name"}}},
        },
    },
)
async def create_share(
    request: Request,
    response: Response,
    share_name: str,
    description: str,
    storage_root: Optional[str] = Query(
        default=None,
        description="Optional storage root URL for the share. Leave empty or omit to use default storage.",
    ),
    workspace_url: str = Depends(get_workspace_url),
) -> ShareInfo:
    """Create a new Delta Sharing share for Databricks-to-Databricks data sharing."""
    # Convert empty string to None for storage_root
    if storage_root is not None and storage_root.strip() == "":
        storage_root = None

    logger.info(
        "Creating share",
        share_name=share_name,
        description=description,
        storage_root=storage_root,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    if not share_name or not share_name.strip():
        logger.warning("Invalid share creation request - empty share name")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Share name must be provided and cannot be empty.",
        )

    # Validate share name format
    if not re.match(r"^[a-zA-Z0-9_-]+$", share_name):
        logger.warning("Invalid share name format", share_name=share_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid share name - Valid names must contain only "
                "alphanumeric characters, underscores, and hyphens, and "
                "cannot contain spaces, periods, forward slashes, or "
                f"control characters: {share_name}"
            ),
        )

    share_resp = get_shares(share_name, workspace_url)

    if share_resp:
        logger.warning("Share already exists", share_name=share_name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Share already exists: {share_name}",
        )

    share_resp = create_share_func(
        share_name=share_name,
        description=description,
        storage_root=storage_root,
        dltshr_workspace_url=workspace_url,
    )

    if isinstance(share_resp, str) and ("is not a valid name" in share_resp):
        logger.error("Share creation failed - invalid name", share_name=share_name, error=share_resp)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Invalid share name - Valid names must contain only "
                f"alphanumeric characters and underscores, and cannot "
                f"contain spaces, periods, forward slashes, or control "
                f"character: {share_name}"
            ),
        )

    response.status_code = status.HTTP_201_CREATED
    logger.info("Share created successfully", share_name=share_name, owner=share_resp.owner)
    if hasattr(request.app.state, "domain_db_pool") and request.app.state.domain_db_pool is not None:
        try:
            from dbrx_api.workflow.db.repository_share import ShareRepository

            repo = ShareRepository(request.app.state.domain_db_pool.pool)
            databricks_share_id = getattr(share_resp, "id", share_resp.name) or share_name
            await repo.create_or_upsert_from_api(
                share_name=share_name,
                databricks_share_id=str(databricks_share_id),
                description=description,
                created_by="api",
            )
            logger.info("Logged share to workflow DB", share_name=share_name)
        except Exception as db_err:
            logger.warning(
                "Failed to log share to workflow DB (Databricks create succeeded)",
                share_name=share_name,
                error=str(db_err),
            )
    return share_resp


@ROUTER_SHARE.put(
    "/shares/{share_name}/dataobject/add",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Share not found",
            "content": {"application/json": {"example": {"Message": "Share not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Data object already exists in share",
            "content": {"application/json": {"example": {"Message": "Data object already exists in share"}}},
        },
    },
)
async def add_data_objects_to_share(
    request: Request,
    share_name: str,
    response: Response,
    objects_to_add: AddDataObjectsRequest = Body(
        ...,
        examples=[
            {
                "tables": ["catalog.schema.table1", "catalog.schema.table2"],
                "views": ["catalog.schema.view1"],
                "schemas": ["catalog.schema"],
            }
        ],
    ),
    workspace_url: str = Depends(get_workspace_url),
) -> ShareInfo:
    """Add data objects (tables, views, schemas) to an existing Delta Sharing share."""
    logger.info(
        "Adding data objects to share",
        share_name=share_name,
        tables=objects_to_add.tables,
        views=objects_to_add.views,
        schemas=objects_to_add.schemas,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    share = get_shares(share_name, workspace_url)

    if not share:
        logger.warning(
            "Share not found for adding data objects",
            share_name=share_name,
            http_status=404,
            http_method=request.method,
            url_path=str(request.url.path),
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Share not found: {share_name}",
        )

    result = add_data_object_to_share(
        share_name=share_name,
        objects_to_add=objects_to_add.model_dump(),
        dltshr_workspace_url=workspace_url,
    )

    # Handle error responses (string messages)
    if isinstance(result, str):
        if "already exists" in result:
            logger.warning(
                "Data object already exists in share",
                share_name=share_name,
                error=result,
                http_status=409,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=result,
            )
        elif "Permission denied" in result:
            logger.warning(
                "Permission denied to add data objects",
                share_name=share_name,
                error=result,
                http_status=403,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        elif "not found" in result or "does not exist" in result:
            logger.warning(
                "Data object not found",
                share_name=share_name,
                error=result,
                http_status=404,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )
        elif "No data objects provided" in result:
            logger.warning(
                "No data objects provided",
                share_name=share_name,
                http_status=400,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )
        elif "Cannot add schemas" in result or "Invalid parameter" in result:
            logger.error(
                "Invalid parameter for adding data objects",
                share_name=share_name,
                error=result,
                http_status=400,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )
        else:
            logger.error(
                "Failed to add data objects to share",
                share_name=share_name,
                error=result,
                http_status=400,
                http_method=request.method,
                url_path=str(request.url.path),
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    response.status_code = status.HTTP_200_OK
    logger.info("Data objects added successfully to share", share_name=share_name)
    await _sync_share_to_db(request, share_name, workspace_url)
    return result


@ROUTER_SHARE.put(
    "/shares/{share_name}/dataobject/revoke",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Share not found",
            "content": {"application/json": {"example": {"Message": "Share not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Failed to revoke data objects",
            "content": {"application/json": {"example": {"Message": "Failed to revoke data objects"}}},
        },
    },
)
async def revoke_data_objects_from_share(
    request: Request,
    share_name: str,
    response: Response,
    objects_to_revoke: AddDataObjectsRequest = Body(
        ...,
        examples=[
            {
                "tables": ["catalog.schema.table1", "catalog.schema.table2"],
                "views": ["catalog.schema.view1"],
                "schemas": ["catalog.schema"],
            }
        ],
    ),
    workspace_url: str = Depends(get_workspace_url),
) -> ShareInfo:
    """Remove data objects (tables, views, schemas) from a Delta Sharing share."""
    logger.info(
        "Revoking data objects from share",
        share_name=share_name,
        tables=objects_to_revoke.tables,
        views=objects_to_revoke.views,
        schemas=objects_to_revoke.schemas,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    share = get_shares(share_name, workspace_url)

    if not share:
        logger.warning("Share not found for revoking data objects", share_name=share_name)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Share not found: {share_name}",
        )

    result = revoke_data_object_from_share(
        share_name=share_name,
        objects_to_revoke=objects_to_revoke.model_dump(),
        dltshr_workspace_url=workspace_url,
    )

    # Handle error responses (string messages)
    if isinstance(result, str):
        if "Permission denied" in result or "User is not an owner" in result:
            logger.warning("Permission denied to revoke data objects", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        elif "not found" in result or "does not exist" in result:
            logger.warning("Data object not found for revocation", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )
        elif "No data objects provided" in result:
            logger.warning("No data objects provided for revocation", share_name=share_name)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )
        elif "Cannot remove schemas" in result or "Invalid parameter" in result:
            logger.error("Invalid parameter for revoking data objects", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )
        else:
            logger.error("Failed to revoke data objects from share", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    response.status_code = status.HTTP_200_OK
    logger.info("Data objects revoked successfully from share", share_name=share_name)
    await _sync_share_to_db(request, share_name, workspace_url)
    return result


@ROUTER_SHARE.put(
    "/shares/{share_name}/recipients/add",
    responses={
        status.HTTP_200_OK: {
            "description": "Recipient added successfully",
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Share or recipient not found",
        },
        status.HTTP_409_CONFLICT: {
            "description": "Recipient already has access to share",
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
        },
    },
)
async def add_recipient_to_share(
    share_name: str,
    recipient_name: str,
    request: Request,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> UpdateSharePermissionsResponse:
    """Grant SELECT permission to a recipient for a Delta Sharing share."""
    logger.info(
        "Adding recipient to share",
        share_name=share_name,
        recipient_name=recipient_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    # Call SDK function directly
    result = adding_recipients_to_share(
        dltshr_workspace_url=workspace_url,
        share_name=share_name,
        recipient_name=recipient_name,
    )

    # Handle error responses (string messages from SDK)
    if isinstance(result, str):
        result_lower = result.lower()
        if "already has" in result_lower or "already exists" in result_lower:
            logger.warning(
                "Recipient already has access to share", share_name=share_name, recipient_name=recipient_name
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=result,
            )
        elif "Permission denied" in result or "not an owner" in result:
            logger.warning("Permission denied to add recipient to share", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        elif "not found" in result or "does not exist" in result:
            logger.warning(
                "Share or recipient not found", share_name=share_name, recipient_name=recipient_name, error=result
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )
        else:
            logger.error(
                "Failed to add recipient to share", share_name=share_name, recipient_name=recipient_name, error=result
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    # Success - return UpdateSharePermissionsResponse object
    response.status_code = status.HTTP_200_OK
    logger.info("Recipient added successfully to share", share_name=share_name, recipient_name=recipient_name)
    await _sync_share_to_db(request, share_name, workspace_url)
    return result


@ROUTER_SHARE.put(
    "/shares/{share_name}/recipients/remove",
    responses={
        status.HTTP_200_OK: {
            "description": "Recipient removed successfully",
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Share or recipient not found",
        },
        status.HTTP_409_CONFLICT: {
            "description": "Recipient already has access to share",
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
        },
    },
)
async def remove_recipients_from_share(
    share_name: str,
    recipient_name: str,
    request: Request,
    response: Response,
    workspace_url: str = Depends(get_workspace_url),
) -> UpdateSharePermissionsResponse:
    """Revoke SELECT permission from a recipient for a Delta Sharing share."""
    logger.info(
        "Removing recipient from share",
        share_name=share_name,
        recipient_name=recipient_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    # Call SDK function directly
    result = removing_recipients_from_share(
        dltshr_workspace_url=workspace_url,
        share_name=share_name,
        recipient_name=recipient_name,
    )

    # Handle error responses (string messages from SDK)
    if isinstance(result, str):
        result.lower()
        if "Permission denied" in result or "not an owner" in result:
            logger.warning("Permission denied to remove recipient from share", share_name=share_name, error=result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=result,
            )
        elif "not found" in result or "does not exist" in result or "does not have access" in result:
            logger.warning(
                "Share or recipient not found", share_name=share_name, recipient_name=recipient_name, error=result
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=result,
            )
        else:
            logger.error(
                "Failed to remove recipient from share",
                share_name=share_name,
                recipient_name=recipient_name,
                error=result,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result,
            )

    # Success - return UpdateSharePermissionsResponse object
    response.status_code = status.HTTP_200_OK
    logger.info("Recipient removed successfully from share", share_name=share_name, recipient_name=recipient_name)
    await _sync_share_to_db(request, share_name, workspace_url)
    return result
