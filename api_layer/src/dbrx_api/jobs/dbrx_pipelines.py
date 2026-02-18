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
from dbrx_api.monitoring.logger import logger


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


def validate_and_prepare_catalog(
    w_client: "WorkspaceClient",
    catalog_name: str,
) -> dict:
    """
    Validate that catalog exists.

    NOTE: This function NO LONGER auto-creates catalogs. Catalogs must be created
    via the dedicated /catalogs/{catalog_name} endpoint before creating pipelines.

    Args:
        w_client: WorkspaceClient instance
        catalog_name: Catalog name

    Returns:
        dict with 'success' (bool), 'message' (str), 'exists' (bool)
    """
    try:
        # Check if catalog exists
        try:
            catalog = w_client.catalogs.get(name=catalog_name)
            logger.info(
                "Catalog exists",
                catalog=catalog_name,
                owner=catalog.owner if catalog else None,
            )
            return {
                "success": True,
                "message": f"Catalog '{catalog_name}' is ready",
                "exists": True,
            }
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg or "catalog_not_found" in error_msg:
                # Catalog doesn't exist - return error
                logger.warning(
                    "Catalog does not exist",
                    catalog=catalog_name,
                )
                return {
                    "success": False,
                    "message": f"Catalog '{catalog_name}' not found. Create it first using: POST /catalogs/{catalog_name}",
                    "exists": False,
                }
            else:
                # Different error (permissions, etc.)
                logger.error(
                    "Error checking catalog",
                    catalog=catalog_name,
                    error=str(e),
                )
                return {
                    "success": False,
                    "message": f"Error accessing catalog '{catalog_name}': {str(e)}",
                    "exists": False,
                }
    except Exception as e:
        logger.error(
            "Unexpected error in validate_and_prepare_catalog",
            catalog=catalog_name,
            error=str(e),
        )
        return {
            "success": False,
            "message": f"Unexpected error validating catalog: {str(e)}",
            "exists": False,
        }


def validate_and_prepare_target_schema(
    w_client: "WorkspaceClient",
    target_catalog_name: str,
    target_schema_name: str,
) -> dict:
    """
    Validate target schema exists, create if it doesn't.

    Args:
        w_client: WorkspaceClient instance
        target_catalog_name: Target catalog name
        target_schema_name: Target schema name

    Returns:
        dict with 'success' (bool), 'message' (str), 'created' (bool)
    """
    try:
        full_schema_name = f"{target_catalog_name}.{target_schema_name}"

        # Check if schema exists
        try:
            schema = w_client.schemas.get(full_name=full_schema_name)
            logger.info(
                "Target schema exists",
                catalog=target_catalog_name,
                schema=target_schema_name,
                owner=schema.owner if schema else None,
            )
            return {
                "success": True,
                "message": f"Target schema '{full_schema_name}' exists",
                "created": False,
            }
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                # Schema doesn't exist, try to create it
                logger.info(
                    "Target schema does not exist, creating it",
                    catalog=target_catalog_name,
                    schema=target_schema_name,
                )

                try:
                    w_client.schemas.create(
                        name=target_schema_name,
                        catalog_name=target_catalog_name,
                        comment="Schema created automatically for DLT pipeline",
                    )
                    logger.info(
                        "Target schema created successfully",
                        catalog=target_catalog_name,
                        schema=target_schema_name,
                    )
                    return {
                        "success": True,
                        "message": f"Target schema '{full_schema_name}' created successfully",
                        "created": True,
                    }
                except Exception as create_error:
                    logger.error(
                        "Failed to create target schema",
                        catalog=target_catalog_name,
                        schema=target_schema_name,
                        error=str(create_error),
                    )
                    return {
                        "success": False,
                        "message": f"Failed to create target schema '{full_schema_name}': {str(create_error)}",
                        "created": False,
                    }
            else:
                # Different error (permissions, etc.)
                logger.error(
                    "Error checking target schema",
                    catalog=target_catalog_name,
                    schema=target_schema_name,
                    error=str(e),
                )
                return {
                    "success": False,
                    "message": f"Error accessing target schema '{full_schema_name}': {str(e)}",
                    "created": False,
                }
    except Exception as e:
        logger.error(
            "Unexpected error in validate_and_prepare_target_schema",
            catalog=target_catalog_name,
            schema=target_schema_name,
            error=str(e),
        )
        return {
            "success": False,
            "message": f"Unexpected error validating target schema: {str(e)}",
            "created": False,
        }


def validate_and_prepare_source_table(
    w_client: "WorkspaceClient",
    source_table: str,
) -> dict:
    """
    Validate source table exists, is accessible, and has CDF enabled.
    Enable CDF if not already enabled.

    Args:
        w_client: WorkspaceClient instance
        source_table: Full table name (catalog.schema.table)

    Returns:
        dict with 'success' (bool), 'message' (str), 'cdf_enabled' (bool), 'cdf_was_enabled' (bool)
    """
    try:
        # Parse table name
        parts = source_table.split(".")
        if len(parts) != 3:
            return {
                "success": False,
                "message": f"Invalid source table format '{source_table}'. Expected format: catalog.schema.table",
                "cdf_enabled": False,
                "cdf_was_enabled": False,
            }

        catalog_name, schema_name, table_name = parts

        # Check if table exists and is accessible
        try:
            table = w_client.tables.get(full_name=source_table)
            logger.info(
                "Source table exists and is accessible",
                table=source_table,
                table_type=table.table_type if table else None,
            )
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                logger.error("Source table does not exist", table=source_table)
                return {
                    "success": False,
                    "message": f"Source table '{source_table}' does not exist",
                    "cdf_enabled": False,
                    "cdf_was_enabled": False,
                }
            else:
                logger.error("Cannot access source table", table=source_table, error=str(e))
                return {
                    "success": False,
                    "message": f"Cannot access source table '{source_table}': {str(e)}",
                    "cdf_enabled": False,
                    "cdf_was_enabled": False,
                }

        # Check if CDF is enabled
        cdf_enabled = False
        if table and table.properties:
            cdf_enabled = table.properties.get("delta.enableChangeDataFeed", "false").lower() == "true"

        if cdf_enabled:
            logger.info("Change Data Feed is already enabled", table=source_table)
            return {
                "success": True,
                "message": f"Source table '{source_table}' exists and CDF is enabled",
                "cdf_enabled": True,
                "cdf_was_enabled": True,
            }
        else:
            # CDF not enabled - must enable it (mandatory for pipelines)
            logger.info("Change Data Feed is not enabled, attempting to enable via SQL warehouse", table=source_table)

            try:
                # Get SQL warehouses and prefer running ones
                warehouses = list(w_client.warehouses.list())
                if not warehouses:
                    logger.error(
                        "No SQL warehouses available for CDF enablement",
                        table=source_table,
                    )
                    return {
                        "success": False,
                        "message": (
                            f"Change Data Feed must be enabled on '{source_table}' for CDC pipelines. "
                            f"No SQL warehouse available to enable it. "
                            f"Please either: (1) Create a SQL warehouse in your workspace, or "
                            f"(2) Enable CDF manually: ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                        ),
                        "cdf_enabled": False,
                        "cdf_was_enabled": False,
                    }

                # Prefer running warehouses, then smallest stopped warehouse
                running_warehouses = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
                selected_warehouse = None

                if running_warehouses:
                    selected_warehouse = running_warehouses[0]
                    logger.info(
                        "Using running SQL warehouse for CDF enablement",
                        warehouse_id=selected_warehouse.id,
                        warehouse_name=selected_warehouse.name if selected_warehouse.name else None,
                        state="RUNNING",
                        table=source_table,
                    )
                else:
                    # No running warehouses - pick smallest one (by cluster_size) to auto-start
                    # Sort by cluster_size (e.g., "2X-Small", "X-Small", "Small", etc.)
                    size_order = {
                        "2X-Small": 1,
                        "X-Small": 2,
                        "Small": 3,
                        "Medium": 4,
                        "Large": 5,
                        "X-Large": 6,
                        "2X-Large": 7,
                        "3X-Large": 8,
                        "4X-Large": 9,
                    }
                    sorted_warehouses = sorted(
                        warehouses, key=lambda w: size_order.get(w.cluster_size, 99) if w.cluster_size else 99
                    )
                    selected_warehouse = sorted_warehouses[0]
                    logger.info(
                        "All SQL warehouses are stopped - will auto-start smallest warehouse for CDF enablement",
                        warehouse_id=selected_warehouse.id,
                        warehouse_name=selected_warehouse.name if selected_warehouse.name else None,
                        warehouse_size=selected_warehouse.cluster_size if selected_warehouse.cluster_size else None,
                        state=selected_warehouse.state.value if selected_warehouse.state else "UNKNOWN",
                        table=source_table,
                    )

                warehouse_id = selected_warehouse.id

                # Execute ALTER TABLE command to enable CDF
                alter_sql = f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"

                logger.info(
                    "Executing ALTER TABLE to enable CDF",
                    table=source_table,
                    warehouse_id=warehouse_id,
                )

                w_client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=alter_sql,
                    catalog=catalog_name,
                    schema=schema_name,
                )

                logger.info("Change Data Feed enabled successfully via SQL warehouse", table=source_table)
                return {
                    "success": True,
                    "message": f"Source table '{source_table}' exists and CDF has been enabled",
                    "cdf_enabled": True,
                    "cdf_was_enabled": False,
                }

            except Exception as enable_error:
                # Failed to enable CDF - this is mandatory, so fail pipeline creation
                error_msg = str(enable_error)
                logger.error(
                    "Failed to enable Change Data Feed via SQL warehouse",
                    table=source_table,
                    error=error_msg,
                )
                return {
                    "success": False,
                    "message": (
                        f"Change Data Feed must be enabled on '{source_table}' for CDC pipelines. "
                        f"Failed to enable automatically: {error_msg}. "
                        f"Please enable it manually: ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                    ),
                    "cdf_enabled": False,
                    "cdf_was_enabled": False,
                }

    except Exception as e:
        logger.error(
            "Unexpected error in validate_and_prepare_source_table",
            table=source_table,
            error=str(e),
        )
        return {
            "success": False,
            "message": f"Unexpected error validating source table: {str(e)}",
            "cdf_enabled": False,
            "cdf_was_enabled": False,
        }


def validate_pipeline_keys(
    w_client: "WorkspaceClient",
    source_table: str,
    keys: str,
) -> dict:
    """
    Validate that the specified keys exist as columns in the source table.

    Args:
        w_client: WorkspaceClient instance
        source_table: Full table name (catalog.schema.table)
        keys: Comma-separated list of column names (e.g., "id,timestamp")

    Returns:
        dict with 'success' (bool), 'message' (str), 'valid_keys' (list), 'invalid_keys' (list)
    """
    try:
        # Parse keys
        key_list = [k.strip() for k in keys.split(",") if k.strip()]

        if not key_list:
            return {
                "success": False,
                "message": "No keys specified",
                "valid_keys": [],
                "invalid_keys": [],
            }

        # Get table schema
        try:
            table = w_client.tables.get(full_name=source_table)

            if not table or not table.columns:
                logger.error("Could not retrieve table columns", table=source_table)
                return {
                    "success": False,
                    "message": f"Could not retrieve columns from source table '{source_table}'",
                    "valid_keys": [],
                    "invalid_keys": key_list,
                }

            # Extract column names (case-insensitive comparison)
            table_columns = {col.name.lower(): col.name for col in table.columns}

            valid_keys = []
            invalid_keys = []

            for key in key_list:
                key_lower = key.lower()
                if key_lower in table_columns:
                    # Use the actual column name from the table (preserves case)
                    valid_keys.append(table_columns[key_lower])
                else:
                    invalid_keys.append(key)

            if invalid_keys:
                logger.warning(
                    "Invalid keys found",
                    table=source_table,
                    valid_keys=valid_keys,
                    invalid_keys=invalid_keys,
                )
                return {
                    "success": False,
                    "message": f"The following keys do not exist in source table '{source_table}': {', '.join(invalid_keys)}",
                    "valid_keys": valid_keys,
                    "invalid_keys": invalid_keys,
                }
            else:
                logger.info(
                    "All keys are valid",
                    table=source_table,
                    keys=valid_keys,
                )
                return {
                    "success": True,
                    "message": f"All keys are valid columns in source table '{source_table}'",
                    "valid_keys": valid_keys,
                    "invalid_keys": [],
                }

        except Exception as e:
            logger.error(
                "Error retrieving table schema",
                table=source_table,
                error=str(e),
            )
            return {
                "success": False,
                "message": f"Error retrieving schema from source table '{source_table}': {str(e)}",
                "valid_keys": [],
                "invalid_keys": key_list,
            }

    except Exception as e:
        logger.error(
            "Unexpected error in validate_pipeline_keys",
            table=source_table,
            keys=keys,
            error=str(e),
        )
        return {
            "success": False,
            "message": f"Unexpected error validating keys: {str(e)}",
            "valid_keys": [],
            "invalid_keys": [],
        }


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
    Create a DLT pipeline with comprehensive validations.

    Validations performed:
    1. Target catalog exists (creates if needed and grants privileges to service principal)
    2. Target schema exists (creates if needed)
    3. Source table exists, is accessible, and has CDF enabled (enables if needed)
    4. Pipeline keys are valid columns in the source table

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

        # Extract required configuration values for validation
        source_table = configuration.get("pipelines.source_table")
        keys = configuration.get("pipelines.keys")

        if not source_table:
            return "Missing required configuration: pipelines.source_table"

        if not keys:
            return "Missing required configuration: pipelines.keys"

        logger.info(
            "Starting pipeline validations",
            pipeline_name=pipeline_name,
            source_table=source_table,
            target_catalog=target_catalog_name,
            target_schema=target_schema_name,
            keys=keys,
        )

        # Validation 1: Check and create target catalog if needed
        catalog_validation = validate_and_prepare_catalog(
            w_client=w_client,
            catalog_name=target_catalog_name,
        )

        if not catalog_validation["success"]:
            logger.error(
                "Target catalog validation failed",
                pipeline_name=pipeline_name,
                error=catalog_validation["message"],
            )
            return catalog_validation["message"]

        logger.info(
            "Target catalog validation passed",
            pipeline_name=pipeline_name,
            catalog_exists=catalog_validation["exists"],
            message=catalog_validation["message"],
        )

        # Validation 2: Check and create target schema if needed
        schema_validation = validate_and_prepare_target_schema(
            w_client=w_client,
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
        )

        if not schema_validation["success"]:
            logger.error(
                "Target schema validation failed",
                pipeline_name=pipeline_name,
                error=schema_validation["message"],
            )
            return schema_validation["message"]

        logger.info(
            "Target schema validation passed",
            pipeline_name=pipeline_name,
            schema_created=schema_validation["created"],
            message=schema_validation["message"],
        )

        # Validation 3: Check source table and enable CDF if needed
        source_validation = validate_and_prepare_source_table(
            w_client=w_client,
            source_table=source_table,
        )

        if not source_validation["success"]:
            logger.error(
                "Source table validation failed",
                pipeline_name=pipeline_name,
                source_table=source_table,
                error=source_validation["message"],
            )
            return source_validation["message"]

        logger.info(
            "Source table validation passed",
            pipeline_name=pipeline_name,
            source_table=source_table,
            cdf_was_enabled=source_validation["cdf_was_enabled"],
            message=source_validation["message"],
        )

        # Validation 4: Verify keys exist in source table
        keys_validation = validate_pipeline_keys(
            w_client=w_client,
            source_table=source_table,
            keys=keys,
        )

        if not keys_validation["success"]:
            logger.error(
                "Pipeline keys validation failed",
                pipeline_name=pipeline_name,
                source_table=source_table,
                keys=keys,
                invalid_keys=keys_validation["invalid_keys"],
                error=keys_validation["message"],
            )
            return keys_validation["message"]

        logger.info(
            "Pipeline keys validation passed",
            pipeline_name=pipeline_name,
            source_table=source_table,
            valid_keys=keys_validation["valid_keys"],
            message=keys_validation["message"],
        )

        # All validations passed, create the pipeline
        logger.info(
            "All validations passed, creating pipeline",
            pipeline_name=pipeline_name,
        )

        return w_client.pipelines.create(
            name=pipeline_name,
            catalog=target_catalog_name,
            target=target_schema_name,
            configuration=configuration,
            root_path="/Workspace/Shared/.bundle/dab_project/prod/files/etl/dlt/pattern",
            continuous=False,
            serverless=serverless,
            libraries=[
                PipelineLibrary(
                    file=FileLibrary(
                        path="/Workspace/Shared/.bundle/dab_project/prod/files/etl/dlt/pattern/pattern-load.py"
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

    # Resource not found - be more specific during create operations
    if "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_lower or "not found" in error_lower:
        if operation == "create":
            # During create, "not found" likely means catalog/schema/library not found, not the pipeline
            return f"Failed to create pipeline '{pipeline_name}': {error_msg}"
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
