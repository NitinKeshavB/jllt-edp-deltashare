"""Module for managing Databricks DLT pipelines for Delta Sharing."""

import time
from datetime import datetime
from datetime import timezone
from typing import List
from typing import Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.pipelines import CreatePipelineResponse
    from databricks.sdk.service.pipelines import FileLibrary
    from databricks.sdk.service.pipelines import GetPipelineResponse
    from databricks.sdk.service.pipelines import Notifications
    from databricks.sdk.service.pipelines import PipelineCluster
    from databricks.sdk.service.pipelines import PipelineLibrary
    from databricks.sdk.service.pipelines import PipelineState
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


def list_pipelines(
    dltshr_workspace_url: str,
    max_results: Optional[int] = None,
) -> List[GetPipelineResponse]:
    """
    List Databricks DLT pipelines with optional filtering.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        max_results: Maximum number of pipelines to return (None = all)
        filter_expr: Optional SQL-like filter (e.g., "name LIKE '%pattern%'")

    Returns:
        List of GetPipelineResponse objects

    Raises:
        Exception: If authentication or API call fails

    Example:
        >>> pipelines = list_pipelines(
        ...     "https://adb-123.azuredatabricks.net/",
        ...     filter_expr="name LIKE '%dlt-pattern%'"
        ... )
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

    all_pipelines = []

    # Use SDK's list_pipelines with automatic pagination
    for pipeline in w_client.pipelines.list_pipelines(max_results=max_results):
        all_pipelines.append(pipeline)

    return all_pipelines


def list_pipelines_with_search_criteria(
    dltshr_workspace_url: str,
    max_results: Optional[int] = None,
    filter_expr: Optional[str] = None,
) -> List[GetPipelineResponse]:
    """
    List Databricks DLT pipelines with optional filtering.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        max_results: Maximum number of pipelines to return (None = all)
        filter_expr: Search string to filter pipeline names (case-insensitive substring match)

    Returns:
        List of GetPipelineResponse objects

    Raises:
        Exception: If authentication or API call fails

    Example:
        >>> pipelines = list_pipelines_with_search_criteria(
        ...     "https://adb-123.azuredatabricks.net/",
        ...     filter_expr="pattern"
        ... )
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

    all_pipelines = []
    filter_expr_name = f"name like '%{filter_expr}%'"
    # Get all pipelines first
    pipelines = w_client.pipelines.list_pipelines(filter=filter_expr_name, max_results=max_results)
    for pipeline in pipelines:
        all_pipelines.append(pipeline)
    return all_pipelines


def get_pipeline_by_name(
    dltshr_workspace_url: str,
    pipeline_name: str,
) -> GetPipelineResponse | None:
    """
    Get details of a specific DLT pipeline by name.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_name: Exact pipeline name (case-sensitive)

    Returns:
        GetPipelineResponse object or None if not found

    Raises:
        Exception: If authentication fails or API error occurs
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Filter by exact name match - list_pipelines returns PipelineStateInfo
        pipelines = w_client.pipelines.list_pipelines(filter=f"name like '{pipeline_name}'")

        # Get the pipeline_id from list, then fetch full GetPipelineResponse
        for pipeline in pipelines:
            # Use pipelines.get() to return GetPipelineResponse instead of PipelineStateInfo
            return w_client.pipelines.get(pipeline_id=pipeline.pipeline_id)

        return None

    except Exception as e:
        error_msg = str(e).lower()
        if "does not exist" in error_msg or "not found" in error_msg:
            return None


def create_pipeline(
    dltshr_workspace_url: str,
    pipeline_name: str,
    target_catalog_name: str,
    target_schema_name: str,
    configuration: dict,
    notifications_list: list,
    tags: dict,
    serverless: bool = False,
) -> CreatePipelineResponse | str:
    """
    Create a DLT pipeline.

    Returns:
        CreatePipelineResponse on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
        pipelines = w_client.pipelines.list_pipelines(filter=f"name like '{pipeline_name}'")

        # Initialize pipeline_id to None
        pipeline_id = None
        for pipeline in pipelines:
            pipeline_id = pipeline.pipeline_id

        if pipeline_id:
            return f"Pipeline already exists: {pipeline_name}"
        else:
            return w_client.pipelines.create(
                name=pipeline_name,
                catalog=target_catalog_name,
                target=target_schema_name,
                configuration=configuration,
                root_path="/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern",
                continuous=False,
                serverless=serverless,
                libraries=[
                    PipelineLibrary(
                        file=FileLibrary(
                            path="/Workspace/Shared/.bundle/dab_project/prod/files/citibike_etl/dlt/pattern/pattern-load.py"
                        )
                    )
                ],
                notifications=[
                    Notifications(
                        email_recipients=notifications_list,
                        alerts=[
                            "on-update-failure",
                            "on-update-fatal-failure",
                            "on-update-success",
                            "on-flow-failure",
                        ],
                    )
                ],
                tags=tags,
            )

    except Exception as e:
        return _handle_pipeline_error(e, "create", pipeline_name)


def delete_pipeline(
    dltshr_workspace_url: str,
    pipeline_id: str,
) -> None | str:
    """
    Delete a DLT pipeline.

    Note: This function assumes the pipeline exists. The caller MUST verify
    pipeline existence before calling this function.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_name: Pipeline name to delete

    Returns:
        None on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
        w_client.pipelines.delete(pipeline_id=pipeline_id)
        return None

    except Exception as e:
        return _handle_pipeline_error(e, "delete", pipeline_id)


def update_pipeline_target_configuration(
    dltshr_workspace_url: str,
    pipeline_id: str,
    pipeline_name: str,
    configuration: list,
    catalog: Optional[str] = None,
    target: Optional[str] = None,
    libraries: Optional[list] = None,
    storage: Optional[str] = None,
    serverless: Optional[bool] = None,
    development: Optional[bool] = None,
    notifications: Optional[list] = None,
    tags: Optional[dict] = None,
) -> None | str:
    """
    Update the target and configuration of a DLT pipeline.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_id: Pipeline ID to update
        pipeline_name: Name of the pipeline (for logging/error messages)
        configuration: Pipeline configuration dictionary
        catalog: Optional catalog name (required for UC pipelines to avoid UC->HMS conversion)
        target: Optional target schema name (required for UC pipelines to avoid UC->HMS conversion)
        libraries: Optional libraries list (required by Databricks API)
        storage: Optional storage location (root folder path for pipeline outputs)
        serverless: Optional serverless compute flag
        development: Optional development mode flag (preserves root_path settings)
        notifications: Optional notifications list (email recipients and AD groups)
        tags: Optional tags dictionary (key-value pairs for resource tagging)

    Returns:
        None on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Build update parameters - always include pipeline_id, name, and configuration
        update_params = {
            "pipeline_id": pipeline_id,
            "name": pipeline_name,  # Required by Databricks
            "configuration": configuration,
        }

        # Include catalog and target if provided (required for UC pipelines)
        if catalog is not None:
            update_params["catalog"] = catalog
        if target is not None:
            update_params["target"] = target

        # Include libraries if provided (required by Databricks API)
        if libraries is not None:
            update_params["libraries"] = libraries

        # Include storage if provided (preserves root folder path)
        if storage is not None:
            update_params["storage"] = storage

        # Include serverless if provided (preserves compute type)
        if serverless is not None:
            update_params["serverless"] = serverless

        # Include development if provided (preserves root_path and dev settings)
        if development is not None:
            update_params["development"] = development

        # Include notifications if provided
        if notifications is not None:
            update_params["notifications"] = notifications

        # Include tags if provided
        # Note: For serverless pipelines, we need to preserve existing cluster configuration
        # or not pass clusters at all if serverless=True
        if tags is not None:
            # If serverless is explicitly True, don't use clusters parameter
            # Serverless pipelines don't support cluster configuration
            if serverless is not True:
                update_params["clusters"] = [PipelineCluster(custom_tags=tags)]

        return w_client.pipelines.update(**update_params)

    except Exception as e:
        return _handle_pipeline_error(e, "update_settings", pipeline_name)


def update_pipeline_notifications_add(
    dltshr_workspace_url: str, pipeline_name: str, notifications_list: list
) -> None | str:
    """
    Add notifications to a DLT pipeline.
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
        pipelines = w_client.pipelines.list_pipelines(filter=f"name like '{pipeline_name}'")

        # Initialize pipeline_id to None
        pipeline_id = None
        for pipeline in pipelines:
            pipeline_id = pipeline.pipeline_id

        # can you add logic to get existing notifications list and add new notifications to it?

        if pipeline_id:
            existing_notifications = w_client.pipelines.get(pipeline_id=pipeline_id).notifications
            if existing_notifications:
                existing_notifications.extend(notifications_list)
            else:
                existing_notifications = notifications_list

            return w_client.pipelines.update(
                pipeline_id=pipeline_id,
                notifications=[
                    Notifications(
                        email_recipients=existing_notifications,
                        alerts=existing_notifications[0].alerts,
                    )
                ],
            )
        else:
            return f"Pipeline not found: {pipeline_name}"

    except Exception as e:
        return _handle_pipeline_error(e, "update_settings", pipeline_name)


def update_pipeline_notifications_remove(
    dltshr_workspace_url: str, pipeline_name: str, notifications_to_remove: list
) -> None | str:
    """
    Add notifications to a DLT pipeline.
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
        pipelines = w_client.pipelines.list_pipelines(filter=f"name like '{pipeline_name}'")

        # Initialize pipeline_id to None
        pipeline_id = None
        for pipeline in pipelines:
            pipeline_id = pipeline.pipeline_id

        # can you add logic to get existing notifications list and add new notifications to it?

        if pipeline_id:
            existing_notifications = w_client.pipelines.get(pipeline_id=pipeline_id).notifications
            if existing_notifications:
                for notification in notifications_to_remove:
                    if notification in existing_notifications:
                        existing_notifications.remove(notification)
                return w_client.pipelines.update(
                    pipeline_id=pipeline_id,
                    notifications=[
                        Notifications(
                            email_recipients=existing_notifications,
                            alerts=existing_notifications[0].alerts,
                        )
                    ],
                )
            else:
                return f"Notifications not found: {notifications_to_remove}"
        else:
            return f"Pipeline not found: {pipeline_name}"

    except Exception as e:
        return _handle_pipeline_error(e, "update_settings", pipeline_name)


def update_pipeline_name(dltshr_workspace_url: str, pipeline_name: str, new_pipeline_name: str) -> None | str:
    """
    Update the name of a DLT pipeline.
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
        pipelines = w_client.pipelines.list_pipelines(filter=f"name like '{pipeline_name}'")

        # Initialize pipeline_id to None
        pipeline_id = None
        for pipeline in pipelines:
            pipeline_id = pipeline.pipeline_id

        if pipeline_id:
            return w_client.pipelines.update(pipeline_id=pipeline_id, name=new_pipeline_name)
        else:
            return f"Pipeline not found: {pipeline_name}"

    except Exception as e:
        return _handle_pipeline_error(e, "update_name", pipeline_name)


def update_pipeline_continuous(dltshr_workspace_url: str, pipeline_name: str, continuous: bool) -> None | str:
    """
    Update the continuous mode of a DLT pipeline while preserving all other settings.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_name: Name of the pipeline to update
        continuous: True for continuous mode, False for triggered mode

    Returns:
        Update response on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    try:
        # Reuse existing get_pipeline_by_name logic
        existing_pipeline = get_pipeline_by_name(dltshr_workspace_url, pipeline_name)

        if existing_pipeline is None:
            return f"Pipeline not found: {pipeline_name}"

        # Initialize workspace client to get full pipeline spec
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get full pipeline specification to preserve all settings
        full_pipeline = w_client.pipelines.get(pipeline_id=existing_pipeline.pipeline_id)

        # Extract existing settings to preserve them
        existing_config = (
            dict(full_pipeline.spec.configuration) if full_pipeline.spec and full_pipeline.spec.configuration else {}
        )
        existing_catalog = full_pipeline.spec.catalog if full_pipeline.spec else None
        existing_target = full_pipeline.spec.target if full_pipeline.spec else None
        existing_libraries = (
            full_pipeline.spec.libraries if full_pipeline.spec and full_pipeline.spec.libraries else None
        )
        existing_storage = full_pipeline.spec.storage if full_pipeline.spec else None
        existing_serverless = full_pipeline.spec.serverless if full_pipeline.spec else None
        existing_development = full_pipeline.spec.development if full_pipeline.spec else None
        existing_notifications = (
            full_pipeline.spec.notifications if full_pipeline.spec and full_pipeline.spec.notifications else None
        )

        # Build update parameters - include all existing settings
        update_params = {
            "pipeline_id": existing_pipeline.pipeline_id,
            "name": pipeline_name,  # Required by Databricks
            "continuous": continuous,  # New continuous mode value
        }

        # Preserve configuration
        if existing_config:
            update_params["configuration"] = existing_config

        # Preserve catalog and target (required for UC pipelines)
        if existing_catalog is not None:
            update_params["catalog"] = existing_catalog
        if existing_target is not None:
            update_params["target"] = existing_target

        # Preserve libraries (required by Databricks API)
        if existing_libraries is not None:
            update_params["libraries"] = existing_libraries

        # Preserve storage (root folder path)
        if existing_storage is not None:
            update_params["storage"] = existing_storage

        # Preserve serverless setting
        if existing_serverless is not None:
            update_params["serverless"] = existing_serverless

        # Preserve development settings (includes root_path)
        if existing_development is not None:
            update_params["development"] = existing_development

        # Preserve notifications
        if existing_notifications is not None:
            update_params["notifications"] = existing_notifications

        # Preserve tags (if not serverless)
        if full_pipeline.spec and full_pipeline.spec.clusters and full_pipeline.spec.clusters[0].custom_tags:
            existing_tags = dict(full_pipeline.spec.clusters[0].custom_tags)
            # Only add tags if not serverless (serverless pipelines don't support clusters parameter)
            if existing_serverless is not True:
                from databricks.sdk.service.pipelines import PipelineCluster

                update_params["clusters"] = [PipelineCluster(custom_tags=existing_tags)]

        # Update the pipeline with preserved settings
        return w_client.pipelines.update(**update_params)

    except Exception as e:
        return _handle_pipeline_error(e, "update_continuous", pipeline_name)


def pipeline_full_refresh(
    dltshr_workspace_url: str,
    pipeline_name: str,
) -> None | str:
    """
    Perform a full refresh of a DLT pipeline.

    If pipeline is running, stops it and waits for it to stop before starting a full refresh.
    If pipeline is not running, starts a full refresh immediately.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        pipeline_name: Name of the pipeline to refresh

    Returns:
        Start update response on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        raise ImportError("Databricks SDK is not available")

    try:
        # Reuse existing get_pipeline_by_name logic
        existing_pipeline = get_pipeline_by_name(dltshr_workspace_url, pipeline_name)

        if existing_pipeline is None:
            return f"Pipeline not found: {pipeline_name}"

        # Initialize workspace client
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get full pipeline state
        full_pipeline = w_client.pipelines.get(pipeline_id=existing_pipeline.pipeline_id)
        pipeline_state = full_pipeline.state

        # If pipeline is running, stop it first and wait
        if pipeline_state in [PipelineState.RUNNING, PipelineState.STARTING, PipelineState.STOPPING]:
            w_client.pipelines.stop(pipeline_id=existing_pipeline.pipeline_id)

            # Wait up to 300 seconds (5 minutes)
            time.sleep(300)
            pipeline_state = w_client.pipelines.get(pipeline_id=existing_pipeline.pipeline_id).state

            if pipeline_state in [PipelineState.IDLE, PipelineState.FAILED]:
                return w_client.pipelines.start_update(pipeline_id=existing_pipeline.pipeline_id, full_refresh=True)
            else:
                # Retry for another 300 seconds (10 minutes total)
                time.sleep(300)
                pipeline_state = w_client.pipelines.get(pipeline_id=existing_pipeline.pipeline_id).state

                if pipeline_state in [PipelineState.IDLE, PipelineState.FAILED]:
                    return w_client.pipelines.start_update(
                        pipeline_id=existing_pipeline.pipeline_id, full_refresh=True
                    )
                else:
                    return f"Pipeline did not stop within 600 seconds (10 minutes). Current state: {pipeline_state}"

        # Pipeline is not running, start full refresh immediately
        return w_client.pipelines.start_update(pipeline_id=existing_pipeline.pipeline_id, full_refresh=True)

    except Exception as e:
        return _handle_pipeline_error(e, "pipeline_full_refresh", pipeline_name)


def _handle_pipeline_error(error: Exception, operation: str, pipeline_name: str) -> str:
    """
    Handle Databricks SDK errors and return user-friendly messages.

    Args:
        error: The caught exception
        operation: Operation being performed
        pipeline_id: Pipeline ID involved

    Returns:
        User-friendly error message
    """
    error_msg = str(error)
    error_lower = error_msg.lower()

    # Permission denied
    if "PERMISSION_DENIED" in error_msg or "permission denied" in error_lower or "not an owner" in error_lower:
        return f"Permission denied for {operation} on pipeline: {pipeline_name}"

    # Resource not found
    if "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_lower or "not found" in error_lower:
        return f"Pipeline not found: {pipeline_name}"

    # Invalid state
    if "INVALID_STATE" in error_msg or "invalid state" in error_lower:
        return f"Pipeline {pipeline_name} is in an invalid state for {operation}"

    # Already running
    if "already running" in error_lower or "concurrent" in error_lower:
        return f"Pipeline {pipeline_name} already has an active update running"

    # Unexpected error - log and return generic message
    print(f"✗ Unexpected error during {operation} on pipeline '{pipeline_name}': {error}")
    return f"Failed to {operation} pipeline: {error_msg}"


# if __name__ == "__main__":
#     all_pipelines = get_pipeline_by_name(dltshr_workspace_url="https://adb-3328600036097005.5.azuredatabricks.net/",
#     pipeline_name="")
#     print(all_pipelines)
