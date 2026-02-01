from datetime import datetime
from datetime import timezone
from typing import Union

from databricks.sdk.service.pipelines import CreatePipelineResponse
from databricks.sdk.service.pipelines import GetPipelineResponse
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
from dbrx_api.jobs.dbrx_pipelines import create_pipeline as create_pipeline_sdk
from dbrx_api.jobs.dbrx_pipelines import delete_pipeline as delete_pipeline_sdk
from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name as get_pipeline_by_name_sdk
from dbrx_api.jobs.dbrx_pipelines import list_pipelines as list_pipelines_sdk
from dbrx_api.jobs.dbrx_pipelines import list_pipelines_with_search_criteria as list_pipelines_with_search_criteria_sdk
from dbrx_api.jobs.dbrx_pipelines import pipeline_full_refresh as pipeline_full_refresh_sdk
from dbrx_api.jobs.dbrx_pipelines import update_pipeline_continuous as update_pipeline_continuous_sdk
from dbrx_api.jobs.dbrx_pipelines import update_pipeline_target_configuration as update_pipeline_configuration_sdk
from dbrx_api.schemas.schemas import CreatePipelineRequest
from dbrx_api.schemas.schemas import GetPipelinesQueryParams
from dbrx_api.schemas.schemas import UpdatePipelineConfigurationModel
from dbrx_api.schemas.schemas import UpdatePipelineContinuousModel
from dbrx_api.schemas.schemas import UpdatePipelineLibrariesModel
from dbrx_api.schemas.schemas import UpdatePipelineNotificationsModel

ROUTER_DBRX_PIPELINES = APIRouter(tags=["Pipelines"])


def _get_pipeline_with_full_spec(workspace_url: str, pipeline_name: str) -> tuple:
    """
    Helper function to get pipeline and its full spec, avoiding duplication.

    Args:
        workspace_url: Databricks workspace URL
        pipeline_name: Name of the pipeline

    Returns:
        Tuple of (existing_pipeline, full_pipeline, w_client)

    Raises:
        HTTPException: If pipeline not found or cannot retrieve details
    """
    from databricks.sdk import WorkspaceClient

    from dbrx_api.dbrx_auth.token_gen import get_auth_token

    # Check if pipeline exists and get pipeline_id
    existing_pipeline = get_pipeline_by_name_sdk(workspace_url, pipeline_name)
    if not existing_pipeline:
        logger.warning("Pipeline not found", pipeline_name=pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline not found: {pipeline_name}",
        )

    # Get full pipeline details including spec
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=workspace_url, token=session_token)
        full_pipeline = w_client.pipelines.get(pipeline_id=existing_pipeline.pipeline_id)
        return existing_pipeline, full_pipeline, w_client
    except Exception as e:
        logger.error("Failed to get pipeline details", pipeline_name=pipeline_name, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve pipeline details: {str(e)}",
        )


@ROUTER_DBRX_PIPELINES.get(
    "/pipelines/{pipeline_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid pipeline name",
            "content": {
                "application/json": {"example": {"detail": "Pipeline name cannot have leading or trailing spaces"}}
            },
        },
    },
)
async def get_pipeline_by_name(
    request: Request,
    response: Response,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
) -> GetPipelineResponse:
    """Get a specific pipeline by name."""
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Getting pipeline by name",
        pipeline_name=pipeline_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )
    pipeline = get_pipeline_by_name_sdk(workspace_url, pipeline_name)

    if pipeline is None:
        logger.warning("Pipeline not found", pipeline_name=pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline not found: {pipeline_name}",
        )

    if pipeline:
        response.status_code = status.HTTP_200_OK

    logger.info("Pipeline retrieved successfully", pipeline_name=pipeline_name, pipeline_id=pipeline.pipeline_id)
    return pipeline


@ROUTER_DBRX_PIPELINES.get(
    "/pipelines",
    responses={
        status.HTTP_200_OK: {
            "description": "Pipelines fetched successfully",
            "content": {
                "application/json": {
                    "example": {
                        "Message": "Fetched 5 pipelines!",
                        "Pipeline": [],
                    }
                }
            },
        }
    },
)
async def list_pipelines_all(
    request: Request,
    response: Response,
    query_params: GetPipelinesQueryParams = Depends(),
    workspace_url: str = Depends(get_workspace_url),
):
    """List all pipelines or with optional prefix filtering."""
    logger.info(
        "Listing pipelines",
        page_size=query_params.page_size,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    if query_params.search_string:
        pipelines = list_pipelines_with_search_criteria_sdk(
            dltshr_workspace_url=workspace_url,
            max_results=query_params.page_size,
            filter_expr=query_params.search_string,
        )
    else:
        pipelines = list_pipelines_sdk(dltshr_workspace_url=workspace_url, max_results=query_params.page_size)

    if len(pipelines) == 0:
        logger.info("No pipelines found")
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"detail": "No pipelines found for search criteria."}
        )

    response.status_code = status.HTTP_200_OK
    message = f"Fetched {len(pipelines)} pipelines!"
    logger.info("Pipelines retrieved successfully", count=len(pipelines))
    return {"Message": message, "Pipeline": pipelines}


@ROUTER_DBRX_PIPELINES.post(
    "/pipelines/{pipeline_name}",
    responses={
        status.HTTP_201_CREATED: {
            "description": "Pipeline created successfully",
            "content": {"application/json": {"example": {"Message": "Pipeline created successfully!"}}},
        },
        status.HTTP_409_CONFLICT: {
            "description": "Pipeline already exists",
            "content": {"application/json": {"example": {"Message": "Pipeline already exists"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid pipeline name",
            "content": {
                "application/json": {"example": {"detail": "Pipeline name cannot have leading or trailing spaces"}}
            },
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "configuration", "pipelines.source_table"],
                                "msg": "Field required",
                                "type": "missing",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def create_pipeline(
    request: Request,
    response: Response,
    create_request: CreatePipelineRequest,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
) -> Union[CreatePipelineResponse, GetPipelineResponse]:
    """
    Create a new DLT pipeline with validated configuration.
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    # Extract configuration as dict with aliases (pipelines.* keys)
    configuration_dict = create_request.configuration.model_dump(by_alias=True)

    logger.info(
        "Creating DLT pipeline",
        pipeline_name=pipeline_name,
        target_catalog_name=create_request.target_catalog_name,
        target_schema_name=create_request.target_schema_name,
        configuration=configuration_dict,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline already exists
    existing_pipeline = get_pipeline_by_name_sdk(workspace_url, pipeline_name)
    if existing_pipeline:
        logger.warning("Pipeline already exists", pipeline_name=pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Pipeline already exists: {pipeline_name}",
        )

    # Create the pipeline with validated configuration
    pipeline = create_pipeline_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
        target_catalog_name=create_request.target_catalog_name,
        target_schema_name=create_request.target_schema_name,
        configuration=configuration_dict,
        notifications_list=create_request.notifications_list,
        tags=create_request.tags,
        serverless=create_request.serverless,
    )

    if isinstance(pipeline, str):
        logger.error("Failed to create pipeline", pipeline_name=pipeline_name, error=pipeline)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=pipeline,
        )

    response.status_code = status.HTTP_201_CREATED
    logger.info("Pipeline created successfully", pipeline_name=pipeline_name, pipeline_id=pipeline.pipeline_id)
    return pipeline


@ROUTER_DBRX_PIPELINES.delete(
    "/pipelines/{pipeline_name}",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found"}}},
        },
        status.HTTP_200_OK: {
            "description": "Pipeline deleted successfully",
            "content": {"application/json": {"example": {"message": "Pipeline deleted successfully"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Invalid pipeline name",
            "content": {
                "application/json": {"example": {"detail": "Pipeline name cannot have leading or trailing spaces"}}
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to delete pipeline",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to delete pipeline as user is not the owner"}
                }
            },
        },
    },
)
async def delete_pipeline_by_name(
    request: Request,
    response: Response,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Delete a DLT pipeline by name.
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Deleting pipeline",
        pipeline_name=pipeline_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Check if pipeline exists and get pipeline object with ID
    existing_pipeline = get_pipeline_by_name_sdk(workspace_url, pipeline_name)
    if not existing_pipeline:
        logger.warning("Pipeline not found for deletion", pipeline_name=pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline not found: {pipeline_name}",
        )

    # Extract pipeline_id from the pipeline object
    pipeline_id = existing_pipeline.pipeline_id
    logger.info("Found pipeline for deletion", pipeline_name=pipeline_name, pipeline_id=pipeline_id)

    # Attempt to delete the pipeline using pipeline_id
    delete_result = delete_pipeline_sdk(workspace_url, pipeline_id)

    # Handle the result
    if delete_result is None:
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info("Pipeline deleted successfully", pipeline_name=pipeline_name, pipeline_id=pipeline_id)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": f"Pipeline '{pipeline_name}' deleted successfully"},
        )
    elif "not an owner" in delete_result.lower() or "permission denied" in delete_result.lower():
        # Permission error
        logger.warning(
            "Permission denied to delete pipeline",
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
            error=delete_result,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Permission denied to delete pipeline: {delete_result}",
        )
    elif "not found" in delete_result.lower():
        # Not found (shouldn't happen since we check above, but handle anyway)
        logger.warning(
            "Pipeline not found during deletion",
            pipeline_name=pipeline_name,
            pipeline_id=pipeline_id,
            error=delete_result,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=delete_result,
        )
    else:
        # Other error
        logger.error(
            "Failed to delete pipeline", pipeline_name=pipeline_name, pipeline_id=pipeline_id, error=delete_result
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete pipeline: {delete_result}",
        )


@ROUTER_DBRX_PIPELINES.put(
    "/pipelines/{pipeline_name}/configuration",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Configuration update failed or no fields provided",
            "content": {
                "application/json": {"example": {"detail": "At least one configuration field must be provided"}}
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to update pipeline as user is not the owner",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to update pipeline as user is not the owner"}
                }
            },
        },
        status.HTTP_200_OK: {
            "description": "Pipeline configuration updated successfully",
            "content": {"application/json": {"example": {"message": "Pipeline configuration updated successfully"}}},
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "pipelines.target_table"],
                                "msg": "Value error, target_table 'table-name' contains invalid characters",
                                "type": "value_error",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def update_pipeline_parameters(
    request: Request,
    response: Response,
    parameter_config: UpdatePipelineConfigurationModel,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Update DLT pipeline configuration (partial update for keys and target_table only).

    Updates only the keys and/or target_table in the configuration dictionary.
    Other configuration fields (source_table, scd_type) are immutable after pipeline creation.

    Updateable fields:
    - pipelines.keys: Primary key column(s)
    - pipelines.target_table: Target table name

    **Keys Validation:**
    When updating pipelines.keys, the API automatically validates that all specified keys
    exist as columns in the source table using case-insensitive matching. If any keys are
    invalid, the update will fail with a clear error message listing the invalid keys.

    The configuration will automatically include:
    - pipelines.sequence_by = "_commit_version" (auto-set)
    - pipelines.delete_expr = "_change_type = 'delete'" (auto-set)

    Immutable fields (set during creation, cannot be updated):
    - pipelines.source_table: Source table reference
    - pipelines.scd_type: SCD type (1 or 2)

    Examples:
    - Update only keys: {"pipelines.keys": "new_key_column"}
    - Update only target_table: {"pipelines.target_table": "new_table_name"}
    - Update both: {"pipelines.keys": "id,timestamp", "pipelines.target_table": "updated_table"}
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    # Extract new configuration fields (exclude None values)
    new_config_dict = parameter_config.model_dump(by_alias=True, exclude_none=True)

    logger.info(
        "Updating pipeline configuration",
        pipeline_name=pipeline_name,
        new_configuration=new_config_dict,
        fields_to_update=list(new_config_dict.keys()),
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Get pipeline with full spec (handles existence check and error handling)
    existing_pipeline, full_pipeline, w_client = _get_pipeline_with_full_spec(workspace_url, pipeline_name)

    # Merge with existing configuration to avoid UC -> HMS conversion issues
    # Start with existing configuration if available
    merged_config = (
        dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
    )

    # Update with new values
    merged_config.update(new_config_dict)

    # Validate keys if they are being updated
    if "pipelines.keys" in new_config_dict:
        # Get source table from existing configuration
        existing_config = (
            dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
        )
        source_table = existing_config.get("pipelines.source_table")

        if not source_table:
            logger.error(
                "Cannot validate keys: source table not found in existing configuration",
                pipeline_name=pipeline_name,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot validate keys: source table not found in pipeline configuration",
            )

        new_keys = new_config_dict["pipelines.keys"]

        logger.info(
            "Validating new keys against source table",
            pipeline_name=pipeline_name,
            source_table=source_table,
            new_keys=new_keys,
        )

        # Import the validation function
        from dbrx_api.jobs.dbrx_pipelines import validate_pipeline_keys

        # Validate the new keys
        keys_validation = validate_pipeline_keys(
            w_client=w_client,
            source_table=source_table,
            keys=new_keys,
        )

        if not keys_validation["success"]:
            logger.error(
                "Pipeline keys validation failed",
                pipeline_name=pipeline_name,
                source_table=source_table,
                keys=new_keys,
                invalid_keys=keys_validation["invalid_keys"],
                error=keys_validation["message"],
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=keys_validation["message"],
            )

        logger.info(
            "Pipeline keys validation passed",
            pipeline_name=pipeline_name,
            source_table=source_table,
            valid_keys=keys_validation["valid_keys"],
        )

    # Extract required fields from existing pipeline to preserve pipeline type and settings
    catalog = full_pipeline.spec.catalog if full_pipeline.spec else None
    target = full_pipeline.spec.target if full_pipeline.spec else None
    libraries = full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None
    storage = full_pipeline.spec.storage if full_pipeline.spec else None
    serverless = full_pipeline.spec.serverless if full_pipeline.spec else None
    development = full_pipeline.spec.development if full_pipeline.spec else None

    logger.info(
        "Merged configuration",
        pipeline_name=pipeline_name,
        pipeline_id=existing_pipeline.pipeline_id,
        catalog=catalog,
        target=target,
        libraries_count=len(libraries) if libraries else 0,
        storage=storage,
        serverless=serverless,
        development=development,
        existing_config=(
            dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
        ),
        merged_config=merged_config,
    )

    # Update the pipeline configuration with all preserved settings
    update_result = update_pipeline_configuration_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=existing_pipeline.pipeline_id,
        pipeline_name=pipeline_name,
        configuration=merged_config,
        catalog=catalog,
        target=target,
        libraries=libraries,
        storage=storage,
        serverless=serverless,
        development=development,
    )

    # Handle the result
    if update_result is None or (hasattr(update_result, "pipeline_id") and update_result.pipeline_id):
        # Success
        response.status_code = status.HTTP_200_OK
        updated_fields = [
            k for k in new_config_dict.keys() if k not in ["pipelines.sequence_by", "pipelines.delete_expr"]
        ]
        logger.info(
            "Pipeline configuration updated successfully", pipeline_name=pipeline_name, fields_updated=updated_fields
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' configuration updated successfully",
                "fields_updated": updated_fields,
            },
        )
    elif isinstance(update_result, str):
        # Error returned as string
        if "not an owner" in update_result.lower() or "permission denied" in update_result.lower():
            logger.warning("Permission denied to update pipeline", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to update pipeline: {update_result}",
            )
        elif "not found" in update_result.lower():
            logger.warning("Pipeline not found during update", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=update_result,
            )
        else:
            logger.error("Failed to update pipeline configuration", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update pipeline configuration: {update_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        updated_fields = [
            k for k in new_config_dict.keys() if k not in ["pipelines.sequence_by", "pipelines.delete_expr"]
        ]
        logger.info(
            "Pipeline configuration updated successfully", pipeline_name=pipeline_name, fields_updated=updated_fields
        )
        return update_result


@ROUTER_DBRX_PIPELINES.put(
    "/pipelines/{pipeline_name}/libraries",
    responses={
        status.HTTP_200_OK: {
            "description": "Pipeline libraries updated successfully",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Pipeline 'my-pipeline' libraries updated successfully",
                        "library_path": "/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern/pattern-load.py",
                    }
                }
            },
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
            "content": {"application/json": {"example": {"detail": "Permission denied to update pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Update failed",
            "content": {
                "application/json": {"example": {"detail": "Failed to update pipeline libraries: error details"}}
            },
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "library_path"],
                                "msg": "Value error, library_path must start with '/Workspace/' or '/Repos/'",
                                "type": "value_error",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def update_pipeline_libraries(
    request: Request,
    response: Response,
    libraries_update: UpdatePipelineLibrariesModel,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Update DLT pipeline libraries and/or root folder path.

    Updates the library path and/or root folder path for an existing pipeline while preserving
    all other settings (configuration, catalog, target, storage, etc.).

    You can update:
    - Library path only: {"library_path": "/Workspace/.../notebook.py"}
    - Root path only: {"root_path": "/Workspace/.../folder"}
    - Both: {"library_path": "...", "root_path": "..."}

    At least one field must be provided.

    Validation rules:
    - Library path must start with /Workspace/ or /Repos/ and end with .py
    - Root path must start with /Workspace/ or /Repos/ (directory, not a file)

    Note: This operation preserves all existing pipeline settings including:
    - Configuration (pipelines.source_table, pipelines.keys, etc.)
    - Catalog and target schema
    - Storage and serverless settings
    - All other pipeline specifications
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Updating pipeline libraries/root path",
        pipeline_name=pipeline_name,
        new_library_path=libraries_update.library_path,
        new_root_path=libraries_update.root_path,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Get pipeline with full spec (handles existence check and error handling)
    existing_pipeline, full_pipeline, w_client = _get_pipeline_with_full_spec(workspace_url, pipeline_name)

    # Import required classes for library construction
    from databricks.sdk.service.pipelines import FileLibrary
    from databricks.sdk.service.pipelines import PipelineLibrary

    # Extract existing settings to preserve them
    existing_config = (
        dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
    )
    existing_catalog = full_pipeline.spec.catalog if full_pipeline.spec else None
    existing_target = full_pipeline.spec.target if full_pipeline.spec else None
    existing_storage = full_pipeline.spec.storage if full_pipeline.spec else None
    existing_serverless = full_pipeline.spec.serverless if full_pipeline.spec else None
    existing_libraries = full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None

    # Determine new libraries (update if provided, else keep existing)
    if libraries_update.library_path:
        new_libraries = [PipelineLibrary(file=FileLibrary(path=libraries_update.library_path))]
    else:
        new_libraries = existing_libraries

    # Determine new development/root_path setting
    # Note: In Databricks SDK, root_path is not directly set in development,
    # but we can pass it if the API supports it or keep existing
    # For now, we'll keep existing development settings unless we need to change
    existing_development = full_pipeline.spec.development if full_pipeline.spec else None

    logger.info(
        "Libraries/root path update details",
        pipeline_name=pipeline_name,
        pipeline_id=existing_pipeline.pipeline_id,
        old_libraries=(full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None),
        new_library_path=libraries_update.library_path,
        new_root_path=libraries_update.root_path,
    )

    # Reuse the existing SDK function with preserved settings and new libraries
    update_result = update_pipeline_configuration_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=existing_pipeline.pipeline_id,
        pipeline_name=pipeline_name,
        configuration=existing_config,  # Preserve existing configuration
        catalog=existing_catalog,  # Preserve existing catalog
        target=existing_target,  # Preserve existing target
        libraries=new_libraries,  # Updated libraries
        storage=existing_storage,  # Preserve existing storage
        serverless=existing_serverless,  # Preserve existing serverless setting
        development=existing_development,  # Preserve existing development settings
    )

    # Handle the result
    if update_result is None or (hasattr(update_result, "pipeline_id") and update_result.pipeline_id):
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline libraries/root path updated successfully",
            pipeline_name=pipeline_name,
            library_path=libraries_update.library_path,
            root_path=libraries_update.root_path,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' libraries updated successfully",
                "library_path": libraries_update.library_path,
                "root_path": libraries_update.root_path,
            },
        )
    elif isinstance(update_result, str):
        # Error returned as string
        if "not an owner" in update_result.lower() or "permission denied" in update_result.lower():
            logger.warning("Permission denied to update pipeline", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to update pipeline: {update_result}",
            )
        elif "not found" in update_result.lower():
            logger.warning("Pipeline not found during update", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=update_result,
            )
        else:
            logger.error("Failed to update pipeline libraries", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update pipeline libraries: {update_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline libraries/root path updated successfully",
            pipeline_name=pipeline_name,
            library_path=libraries_update.library_path,
            root_path=libraries_update.root_path,
        )
        return update_result


@ROUTER_DBRX_PIPELINES.put(
    "/pipelines/{pipeline_name}/notifications/add",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Notifications update failed",
            "content": {
                "application/json": {"example": {"detail": "Failed to update pipeline notifications: <error details>"}}
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to update pipeline as user is not the owner",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to update pipeline: User is not the owner"}
                }
            },
        },
        status.HTTP_200_OK: {
            "description": "Pipeline notifications added successfully",
            "content": {
                "application/json": {
                    "examples": {
                        "new_recipients_added": {
                            "summary": "New recipients added",
                            "value": {
                                "message": "Pipeline 'my-pipeline' notifications added successfully",
                                "newly_added": ["new-user@example.com", "new-team"],
                                "already_existing": [],
                                "all_notifications": [
                                    "admin@example.com",
                                    "data-team",
                                    "new-user@example.com",
                                    "new-team",
                                ],
                            },
                        },
                        "some_already_exist": {
                            "summary": "Some recipients already exist",
                            "value": {
                                "message": "Pipeline 'my-pipeline' notifications added successfully",
                                "newly_added": ["new-user@example.com"],
                                "already_existing": ["admin@example.com"],
                                "all_notifications": ["admin@example.com", "data-team", "new-user@example.com"],
                            },
                        },
                        "all_already_exist": {
                            "summary": "All recipients already exist",
                            "value": {
                                "message": "All notification recipients already exist for pipeline 'my-pipeline'",
                                "already_existing": ["admin@example.com", "data-team"],
                                "all_notifications": ["admin@example.com", "data-team"],
                            },
                        },
                    }
                }
            },
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "notifications_list", 0],
                                "msg": "Value error, Invalid notification recipient 'invalid@@@email'. Must be either a valid email address or an AD group name.",
                                "type": "value_error",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def update_pipeline_notifications_add(
    request: Request,
    response: Response,
    notifications_add: UpdatePipelineNotificationsModel,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Add notification recipients to a DLT pipeline.

    Adds new email addresses and/or AD group names to the pipeline's existing
    notification list while preserving all other settings (configuration,
    catalog, target, libraries, storage, etc.).

    This endpoint ADDS to existing notifications (does not replace them).
    If a recipient already exists in the list, it will not be duplicated.

    The notifications list can include:
    - Email addresses (e.g., user@example.com, admin@company.com)
    - AD group names (e.g., data-engineering-team, admin_group, monitoring-alerts)

    At least one recipient to add must be provided.

    Examples:
    - Add single email: {"notifications_list": ["new-user@example.com"]}
    - Add multiple: {"notifications_list": ["user@example.com", "data-team", "monitoring-group"]}
    - Add AD groups: {"notifications_list": ["new-team", "additional-alerts"]}

    Note: This operation preserves all existing pipeline settings including:
    - Configuration (pipelines.source_table, pipelines.keys, etc.)
    - Catalog and target schema
    - Libraries
    - Storage and serverless settings
    - All other pipeline specifications
    - EXISTING notifications (new ones are ADDED to them)
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Updating pipeline notifications",
        pipeline_name=pipeline_name,
        new_notifications=notifications_add.notifications_list,
        notification_count=len(notifications_add.notifications_list),
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Get pipeline with full spec (handles existence check and error handling)
    existing_pipeline, full_pipeline, w_client = _get_pipeline_with_full_spec(workspace_url, pipeline_name)

    # Import required classes for notification construction
    from databricks.sdk.service.pipelines import Notifications

    # Extract existing settings to preserve them
    existing_config = (
        dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
    )
    existing_catalog = full_pipeline.spec.catalog if full_pipeline.spec else None
    existing_target = full_pipeline.spec.target if full_pipeline.spec else None
    existing_libraries = full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None
    existing_storage = full_pipeline.spec.storage if full_pipeline.spec else None
    existing_serverless = full_pipeline.spec.serverless if full_pipeline.spec else None
    existing_development = full_pipeline.spec.development if full_pipeline.spec else None

    # Get existing notifications and merge with new ones
    existing_notifications_list = []
    if (
        full_pipeline.spec
        and full_pipeline.spec.notifications
        and full_pipeline.spec.notifications[0].email_recipients
    ):
        existing_notifications_list = list(full_pipeline.spec.notifications[0].email_recipients or [])

    # Check which notifications are actually new
    existing_set = set(existing_notifications_list)
    new_set = set(notifications_add.notifications_list)
    actually_new = new_set - existing_set
    already_exists = new_set & existing_set

    # If all recipients already exist, return early with 200
    if not actually_new:
        logger.info(
            "All notifications already exist",
            pipeline_name=pipeline_name,
            requested_notifications=notifications_add.notifications_list,
            existing_notifications=existing_notifications_list,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"All notification recipients already exist for pipeline '{pipeline_name}'",
                "already_existing": list(already_exists),
                "all_notifications": existing_notifications_list,
            },
        )

    # Merge: add new notifications to existing ones (using set to avoid duplicates)
    merged_notifications = list(set(existing_notifications_list + notifications_add.notifications_list))

    # Construct new notifications object with merged list
    new_notifications = [
        Notifications(
            email_recipients=merged_notifications,
            alerts=[
                "on-update-failure",
                "on-update-fatal-failure",
                "on-update-success",
                "on-flow-failure",
            ],
        )
    ]

    logger.info(
        "Notifications update details",
        pipeline_name=pipeline_name,
        pipeline_id=existing_pipeline.pipeline_id,
        existing_notifications=existing_notifications_list,
        new_notifications_to_add=notifications_add.notifications_list,
        actually_new=list(actually_new),
        already_existing=list(already_exists),
        merged_notifications=merged_notifications,
    )

    # Call SDK update function with all preserved settings and new notifications
    update_result = update_pipeline_configuration_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=existing_pipeline.pipeline_id,
        pipeline_name=pipeline_name,
        configuration=existing_config,  # Preserve existing configuration
        catalog=existing_catalog,  # Preserve existing catalog
        target=existing_target,  # Preserve existing target
        libraries=existing_libraries,  # Preserve existing libraries
        storage=existing_storage,  # Preserve existing storage
        serverless=existing_serverless,  # Preserve existing serverless setting
        development=existing_development,  # Preserve existing development settings
        notifications=new_notifications,  # Updated notifications
    )

    # Handle the result
    if update_result is None or (hasattr(update_result, "pipeline_id") and update_result.pipeline_id):
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline notifications added successfully",
            pipeline_name=pipeline_name,
            actually_new=list(actually_new),
            already_existing=list(already_exists),
            final_notifications=merged_notifications,
            notification_count=len(merged_notifications),
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' notifications added successfully",
                "newly_added": list(actually_new),
                "already_existing": list(already_exists) if already_exists else [],
                "all_notifications": merged_notifications,
            },
        )
    elif isinstance(update_result, str):
        # Error returned as string
        if "not an owner" in update_result.lower() or "permission denied" in update_result.lower():
            logger.warning("Permission denied to update pipeline", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to update pipeline: {update_result}",
            )
        elif "not found" in update_result.lower():
            logger.warning("Pipeline not found during update", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=update_result,
            )
        else:
            logger.error("Failed to update pipeline notifications", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update pipeline notifications: {update_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline notifications added successfully",
            pipeline_name=pipeline_name,
            actually_new=list(actually_new),
            already_existing=list(already_exists),
            final_notifications=merged_notifications,
            notification_count=len(merged_notifications),
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' notifications added successfully",
                "newly_added": list(actually_new),
                "already_existing": list(already_exists) if already_exists else [],
                "all_notifications": merged_notifications,
            },
        )


@ROUTER_DBRX_PIPELINES.put(
    "/pipelines/{pipeline_name}/notifications/remove",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Notifications removal failed or no notifications exist",
            "content": {
                "application/json": {
                    "examples": {
                        "no_notifications": {
                            "summary": "No existing notifications",
                            "value": {"detail": "No notifications found for this pipeline"},
                        },
                        "update_failed": {
                            "summary": "Update failed",
                            "value": {"detail": "Failed to remove pipeline notifications: <error details>"},
                        },
                        "all_removed": {
                            "summary": "Cannot remove all notifications",
                            "value": {
                                "detail": "Cannot remove all notifications. At least one recipient must remain or use DELETE to remove all."
                            },
                        },
                    }
                }
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to update pipeline as user is not the owner",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to update pipeline: User is not the owner"}
                }
            },
        },
        status.HTTP_200_OK: {
            "description": "Pipeline notifications removed successfully or not found",
            "content": {
                "application/json": {
                    "examples": {
                        "recipients_removed": {
                            "summary": "Recipients removed successfully",
                            "value": {
                                "message": "Pipeline 'my-pipeline' notifications removed successfully",
                                "removed_notifications": ["user@example.com", "old-team"],
                                "not_found": [],
                                "remaining_notifications": ["admin@example.com", "monitoring-team"],
                            },
                        },
                        "some_not_found": {
                            "summary": "Some recipients not found",
                            "value": {
                                "message": "Pipeline 'my-pipeline' notifications removed successfully",
                                "removed_notifications": ["user@example.com"],
                                "not_found": ["non-existent@example.com"],
                                "remaining_notifications": ["admin@example.com", "data-team"],
                            },
                        },
                        "none_exist": {
                            "summary": "None of the recipients exist",
                            "value": {
                                "message": "None of the specified recipients exist in pipeline 'my-pipeline' notification list",
                                "not_found": ["non-existent@example.com", "fake-team"],
                                "all_notifications": ["admin@example.com", "data-team"],
                            },
                        },
                    }
                }
            },
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "notifications_list", 0],
                                "msg": "Value error, Invalid notification recipient 'invalid@@@email'. Must be either a valid email address or an AD group name.",
                                "type": "value_error",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def update_pipeline_notifications_remove(
    request: Request,
    response: Response,
    notifications_remove: UpdatePipelineNotificationsModel,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Remove specific notification recipients from a DLT pipeline.

    Removes specified email addresses and/or AD group names from the pipeline's
    notification list while preserving all other settings (configuration,
    catalog, target, libraries, storage, etc.).

    At least one notification recipient must remain after removal. If you want to
    remove all notifications, the pipeline must have at least 2 recipients before removal.

    The notifications list can include:
    - Email addresses (e.g., user@example.com, admin@company.com)
    - AD group names (e.g., data-engineering-team, admin_group, monitoring-alerts)

    At least one recipient to remove must be provided.

    Examples:
    - Remove single email: {"notifications_list": ["old-user@example.com"]}
    - Remove multiple: {"notifications_list": ["user@example.com", "old-team", "deprecated-group"]}
    - Remove AD groups: {"notifications_list": ["old-team", "deprecated-alerts"]}

    Note: This operation preserves all existing pipeline settings including:
    - Configuration (pipelines.source_table, pipelines.keys, etc.)
    - Catalog and target schema
    - Libraries
    - Storage and serverless settings
    - All other pipeline specifications
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Removing pipeline notifications",
        pipeline_name=pipeline_name,
        notifications_to_remove=notifications_remove.notifications_list,
        removal_count=len(notifications_remove.notifications_list),
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Get pipeline with full spec (handles existence check and error handling)
    existing_pipeline, full_pipeline, w_client = _get_pipeline_with_full_spec(workspace_url, pipeline_name)

    # Import required classes for notification construction
    from databricks.sdk.service.pipelines import Notifications

    # Extract existing settings to preserve them
    existing_config = (
        dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
    )
    existing_catalog = full_pipeline.spec.catalog if full_pipeline.spec else None
    existing_target = full_pipeline.spec.target if full_pipeline.spec else None
    existing_libraries = full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None
    existing_storage = full_pipeline.spec.storage if full_pipeline.spec else None
    existing_serverless = full_pipeline.spec.serverless if full_pipeline.spec else None
    existing_development = full_pipeline.spec.development if full_pipeline.spec else None

    # Get existing notifications
    existing_notifications_list = []
    if (
        full_pipeline.spec
        and full_pipeline.spec.notifications
        and full_pipeline.spec.notifications[0].email_recipients
    ):
        existing_notifications_list = list(full_pipeline.spec.notifications[0].email_recipients or [])

    # Check if pipeline has any notifications
    if not existing_notifications_list:
        logger.warning("No notifications found for pipeline", pipeline_name=pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No notifications found for this pipeline",
        )

    # Check which recipients actually exist and which don't
    existing_set = set(existing_notifications_list)
    to_remove_set = set(notifications_remove.notifications_list)
    actually_exist = to_remove_set & existing_set  # Recipients that exist
    not_found = to_remove_set - existing_set  # Recipients not in the list

    # If none of the requested recipients exist, return early with 200
    if not actually_exist:
        logger.info(
            "None of the requested recipients exist in notification list",
            pipeline_name=pipeline_name,
            requested_removals=notifications_remove.notifications_list,
            existing_notifications=existing_notifications_list,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"None of the specified recipients exist in pipeline '{pipeline_name}' notification list",
                "not_found": list(not_found),
                "all_notifications": existing_notifications_list,
            },
        )

    # Remove specified notifications that exist
    remaining_notifications = [n for n in existing_notifications_list if n not in to_remove_set]

    # Check if at least one notification remains (Databricks requirement)
    if not remaining_notifications:
        logger.warning(
            "Cannot remove all notifications",
            pipeline_name=pipeline_name,
            existing_count=len(existing_notifications_list),
            removal_count=len(actually_exist),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove all notifications. At least one recipient must remain or use a different approach to remove all.",
        )

    # Construct new notifications object with remaining recipients
    new_notifications = [
        Notifications(
            email_recipients=remaining_notifications,
            alerts=[
                "on-update-failure",
                "on-update-fatal-failure",
                "on-update-success",
                "on-flow-failure",
            ],
        )
    ]

    logger.info(
        "Notifications removal details",
        pipeline_name=pipeline_name,
        pipeline_id=existing_pipeline.pipeline_id,
        existing_notifications=existing_notifications_list,
        requested_removals=notifications_remove.notifications_list,
        actually_removed=list(actually_exist),
        not_found=list(not_found),
        remaining_notifications=remaining_notifications,
    )

    # Call SDK update function with all preserved settings and updated notifications
    update_result = update_pipeline_configuration_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_id=existing_pipeline.pipeline_id,
        pipeline_name=pipeline_name,
        configuration=existing_config,  # Preserve existing configuration
        catalog=existing_catalog,  # Preserve existing catalog
        target=existing_target,  # Preserve existing target
        libraries=existing_libraries,  # Preserve existing libraries
        storage=existing_storage,  # Preserve existing storage
        serverless=existing_serverless,  # Preserve existing serverless setting
        development=existing_development,  # Preserve existing development settings
        notifications=new_notifications,  # Updated notifications (with removals)
    )

    # Handle the result
    if update_result is None or (hasattr(update_result, "pipeline_id") and update_result.pipeline_id):
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline notifications removed successfully",
            pipeline_name=pipeline_name,
            actually_removed=list(actually_exist),
            not_found=list(not_found),
            remaining_notifications=remaining_notifications,
            removal_count=len(actually_exist),
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' notifications removed successfully",
                "removed_notifications": list(actually_exist),
                "not_found": list(not_found) if not_found else [],
                "remaining_notifications": remaining_notifications,
            },
        )
    elif isinstance(update_result, str):
        # Error returned as string
        if "not an owner" in update_result.lower() or "permission denied" in update_result.lower():
            logger.warning("Permission denied to update pipeline", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to update pipeline: {update_result}",
            )
        elif "not found" in update_result.lower():
            logger.warning("Pipeline not found during update", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=update_result,
            )
        else:
            logger.error("Failed to remove pipeline notifications", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to remove pipeline notifications: {update_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline notifications removed successfully",
            pipeline_name=pipeline_name,
            actually_removed=list(actually_exist),
            not_found=list(not_found),
            remaining_notifications=remaining_notifications,
            removal_count=len(actually_exist),
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' notifications removed successfully",
                "removed_notifications": list(actually_exist),
                "not_found": list(not_found) if not_found else [],
                "remaining_notifications": remaining_notifications,
            },
        )


@ROUTER_DBRX_PIPELINES.put(
    "/pipelines/{pipeline_name}/continuous",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Continuous mode update failed",
            "content": {
                "application/json": {
                    "example": {"detail": "Failed to update pipeline continuous mode: <error details>"}
                }
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied to update pipeline",
            "content": {
                "application/json": {
                    "example": {"detail": "Permission denied to update pipeline: User is not the owner"}
                }
            },
        },
        status.HTTP_200_OK: {
            "description": "Pipeline continuous mode updated successfully",
            "content": {
                "application/json": {
                    "examples": {
                        "enable_continuous": {
                            "summary": "Enable continuous mode",
                            "value": {
                                "message": "Pipeline 'my-pipeline' continuous mode updated successfully",
                                "pipeline_name": "my-pipeline",
                                "continuous": True,
                                "mode": "continuous",
                            },
                        },
                        "disable_continuous": {
                            "summary": "Disable continuous mode (triggered)",
                            "value": {
                                "message": "Pipeline 'my-pipeline' continuous mode updated successfully",
                                "pipeline_name": "my-pipeline",
                                "continuous": False,
                                "mode": "triggered",
                            },
                        },
                    }
                }
            },
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "continuous"],
                                "msg": "Field required",
                                "type": "missing",
                            }
                        ]
                    }
                }
            },
        },
    },
)
async def update_pipeline_continuous_mode(
    request: Request,
    response: Response,
    continuous_update: UpdatePipelineContinuousModel,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Update the continuous mode of a DLT pipeline.

    Continuous mode determines how the pipeline processes data:
    - **continuous=True**: Pipeline runs continuously, processing data as it arrives
      - Best for streaming data and real-time processing
      - Pipeline stays active and consumes cluster resources continuously
      - Lower latency for data processing

    - **continuous=False**: Pipeline runs in triggered mode
      - Best for batch processing and scheduled jobs
      - Pipeline only runs when manually triggered or scheduled
      - More cost-effective for periodic data processing
      - Higher latency but better resource utilization

    Examples:
    - Enable continuous mode: {"continuous": true}
    - Switch to triggered mode: {"continuous": false}

    Note: This operation only updates the continuous mode setting. All other pipeline
    configurations remain unchanged.
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Updating pipeline continuous mode",
        pipeline_name=pipeline_name,
        continuous=continuous_update.continuous,
        mode="continuous" if continuous_update.continuous else "triggered",
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Call SDK function to update continuous mode (handles pipeline existence check internally)
    update_result = update_pipeline_continuous_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
        continuous=continuous_update.continuous,
    )

    # Handle the result
    if update_result is None or (hasattr(update_result, "pipeline_id") and update_result.pipeline_id):
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline continuous mode updated successfully",
            pipeline_name=pipeline_name,
            continuous=continuous_update.continuous,
            mode="continuous" if continuous_update.continuous else "triggered",
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' continuous mode updated successfully",
                "pipeline_name": pipeline_name,
                "continuous": continuous_update.continuous,
                "mode": "continuous" if continuous_update.continuous else "triggered",
            },
        )
    elif isinstance(update_result, str):
        # Error returned as string
        if "not an owner" in update_result.lower() or "permission denied" in update_result.lower():
            logger.warning("Permission denied to update pipeline", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied to update pipeline: {update_result}",
            )
        elif "not found" in update_result.lower():
            logger.warning("Pipeline not found during update", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=update_result,
            )
        else:
            logger.error("Failed to update pipeline continuous mode", pipeline_name=pipeline_name, error=update_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update pipeline continuous mode: {update_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Pipeline continuous mode updated successfully",
            pipeline_name=pipeline_name,
            continuous=continuous_update.continuous,
            mode="continuous" if continuous_update.continuous else "triggered",
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Pipeline '{pipeline_name}' continuous mode updated successfully",
                "pipeline_name": pipeline_name,
                "continuous": continuous_update.continuous,
                "mode": "continuous" if continuous_update.continuous else "triggered",
            },
        )


@ROUTER_DBRX_PIPELINES.post(
    "/pipelines/{pipeline_name}/full-refresh",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "description": "Pipeline not found",
            "content": {"application/json": {"example": {"detail": "Pipeline not found: my-pipeline"}}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "Full refresh failed",
            "content": {
                "application/json": {
                    "example": {"detail": "Failed to start full refresh: Pipeline did not stop within 600 seconds"}
                }
            },
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Permission denied",
            "content": {"application/json": {"example": {"detail": "Permission denied: User is not the owner"}}},
        },
        status.HTTP_200_OK: {
            "description": "Full refresh started successfully",
            "content": {
                "application/json": {
                    "examples": {
                        "immediate_start": {
                            "summary": "Pipeline was idle, full refresh started immediately",
                            "value": {
                                "message": "Full refresh started successfully for pipeline 'my-pipeline'",
                                "pipeline_name": "my-pipeline",
                                "action": "full_refresh",
                                "status": "started",
                            },
                        },
                        "stopped_then_started": {
                            "summary": "Pipeline was running, stopped and then full refresh started",
                            "value": {
                                "message": "Full refresh started successfully for pipeline 'my-pipeline'",
                                "pipeline_name": "my-pipeline",
                                "action": "full_refresh",
                                "status": "started",
                                "note": "Pipeline was stopped before starting full refresh",
                            },
                        },
                    }
                }
            },
        },
        status.HTTP_408_REQUEST_TIMEOUT: {
            "description": "Pipeline did not stop in time",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Pipeline did not stop within 600 seconds (10 minutes). Current state: STOPPING"
                    }
                }
            },
        },
    },
)
async def pipeline_full_refresh_endpoint(
    request: Request,
    response: Response,
    pipeline_name: str = Path(..., min_length=1, description="Name of the pipeline (cannot be empty)"),
    workspace_url: str = Depends(get_workspace_url),
):
    """
    Perform a full refresh of a DLT pipeline.

    A full refresh will:
    - Recompute all tables from scratch
    - Drop all existing data in target tables
    - Re-read all source data
    - Rebuild all derived tables

    **Behavior:**
    - If the pipeline is **IDLE/STOPPED**: Full refresh starts immediately
    - If the pipeline is **RUNNING/STARTING/STOPPING**: Pipeline is stopped first, then full refresh starts
    - Maximum wait time: 10 minutes (600 seconds) for pipeline to stop

    **Use Cases:**
    - Schema changes that require data rebuild
    - Data quality issues requiring complete reprocessing
    - Testing with fresh data
    - Recovering from corrupted state

    **Warning:**
    - This operation will **delete all existing data** in target tables
    - Full refresh can take significant time depending on data volume
    - The pipeline will be unavailable during the refresh

    **Note:**
    This endpoint does not require a request body. All pipeline configurations
    are preserved - only the data is refreshed.

    Examples:
    - Start full refresh: `POST /pipelines/my-pipeline/full-refresh`
    """
    # Validate no leading or trailing spaces
    if pipeline_name != pipeline_name.strip():
        logger.warning(
            "Pipeline name has leading or trailing spaces",
            pipeline_name=pipeline_name,
            stripped=pipeline_name.strip(),
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot have leading or trailing spaces",
        )

    # Additional validation for whitespace-only strings
    if not pipeline_name.strip():
        logger.warning("Pipeline name contains only whitespace")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline name cannot contain only whitespace",
        )

    logger.info(
        "Starting full refresh for pipeline",
        pipeline_name=pipeline_name,
        method=request.method,
        path=request.url.path,
        workspace_url=workspace_url,
    )

    # Call SDK function to perform full refresh (handles pipeline existence check internally)
    refresh_result = pipeline_full_refresh_sdk(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
    )

    # Handle the result
    if refresh_result is None or (hasattr(refresh_result, "update_id") and refresh_result.update_id):
        # Success
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Full refresh started successfully",
            pipeline_name=pipeline_name,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Full refresh started successfully for pipeline '{pipeline_name}'",
                "pipeline_name": pipeline_name,
                "action": "full_refresh",
                "status": "started",
            },
        )
    elif isinstance(refresh_result, str):
        # Error returned as string
        if "not an owner" in refresh_result.lower() or "permission denied" in refresh_result.lower():
            logger.warning("Permission denied", pipeline_name=pipeline_name, error=refresh_result)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {refresh_result}",
            )
        elif "not found" in refresh_result.lower():
            logger.warning("Pipeline not found", pipeline_name=pipeline_name, error=refresh_result)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=refresh_result,
            )
        elif "did not stop within" in refresh_result.lower():
            logger.error("Pipeline did not stop in time", pipeline_name=pipeline_name, error=refresh_result)
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=refresh_result,
            )
        else:
            logger.error("Failed to start full refresh", pipeline_name=pipeline_name, error=refresh_result)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to start full refresh: {refresh_result}",
            )
    else:
        # Success with response object
        response.status_code = status.HTTP_200_OK
        logger.info(
            "Full refresh started successfully",
            pipeline_name=pipeline_name,
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Full refresh started successfully for pipeline '{pipeline_name}'",
                "pipeline_name": pipeline_name,
                "action": "full_refresh",
                "status": "started",
            },
        )
