"""Module for managing Databricks Unity Catalogs."""

from datetime import datetime
from datetime import timezone

try:
    from databricks.sdk import WorkspaceClient

    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False

from dbrx_api.dbrx_auth.token_gen import get_auth_token
from dbrx_api.monitoring.logger import logger


def create_catalog(
    workspace_url: str,
    catalog_name: str,
    comment: str = "Catalog created via Delta Share API",
    external_location: str = None,
) -> dict:
    """
    Create a Unity Catalog with optional external location.

    Args:
        workspace_url: Databricks workspace URL
        catalog_name: Name of the catalog to create
        comment: Optional comment for the catalog
        external_location: Optional external location name (must exist in Unity Catalog)

    Returns:
        dict with 'success' (bool), 'message' (str), 'created' (bool)

    Example:
        >>> result = create_catalog(
        ...     workspace_url="https://adb-123.azuredatabricks.net",
        ...     catalog_name="my_catalog",
        ...     comment="My catalog",
        ...     external_location="my_external_location"
        ... )
        >>> print(result)
        {'success': True, 'message': 'Catalog created...', 'created': True}
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return {
            "success": False,
            "message": "Databricks SDK is not available",
            "created": False,
        }

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        # Check if catalog already exists
        try:
            catalog = w_client.catalogs.get(name=catalog_name)
            logger.info(
                "Catalog already exists",
                catalog=catalog_name,
                owner=catalog.owner if catalog else None,
            )
            return {
                "success": False,
                "message": f"Catalog '{catalog_name}' already exists",
                "created": False,
            }
        except Exception as e:
            error_msg = str(e).lower()
            if (
                "does not exist" not in error_msg
                and "not found" not in error_msg
                and "catalog_not_found" not in error_msg
            ):
                # Different error (permissions, etc.)
                logger.error(
                    "Error checking catalog",
                    catalog=catalog_name,
                    error=str(e),
                )
                return {
                    "success": False,
                    "message": f"Error accessing catalog '{catalog_name}': {str(e)}",
                    "created": False,
                }

        # Catalog doesn't exist, create it
        logger.info("Catalog does not exist, creating it", catalog=catalog_name)

        # Prepare storage_root parameter
        storage_root = None
        if external_location:
            # Look up the external location to get its storage path
            try:
                ext_loc = w_client.external_locations.get(name=external_location)
                storage_root = ext_loc.url
                logger.info(
                    "Retrieved external location details",
                    external_location=external_location,
                    storage_url=storage_root,
                )
            except Exception as ext_loc_error:
                error_msg = str(ext_loc_error).lower()
                if "does not exist" in error_msg or "not found" in error_msg:
                    logger.error(
                        "External location not found",
                        external_location=external_location,
                    )
                    return {
                        "success": False,
                        "message": (
                            f"External location '{external_location}' does not exist. "
                            f"Please create the external location first or use a different one."
                        ),
                        "created": False,
                    }
                else:
                    logger.error(
                        "Failed to retrieve external location",
                        external_location=external_location,
                        error=str(ext_loc_error),
                    )
                    return {
                        "success": False,
                        "message": f"Failed to access external location '{external_location}': {str(ext_loc_error)}",
                        "created": False,
                    }

        # Create catalog using Databricks SDK
        try:
            logger.info(
                "Creating catalog via SDK",
                catalog=catalog_name,
                storage_root=storage_root,
                comment=comment,
            )

            w_client.catalogs.create(
                name=catalog_name,
                comment=comment,
                storage_root=storage_root,
            )

            logger.info(
                "Catalog created successfully via SDK",
                catalog=catalog_name,
            )

            # Verify catalog was created
            try:
                catalog = w_client.catalogs.get(name=catalog_name)
                logger.info(
                    "Catalog creation verified",
                    catalog=catalog_name,
                    owner=catalog.owner if catalog else None,
                )
            except Exception as verify_error:
                logger.warning(
                    "Catalog verification check failed but catalog may exist",
                    catalog=catalog_name,
                    error=str(verify_error),
                )

        except Exception as create_error:
            error_msg = str(create_error)
            error_lower = error_msg.lower()

            logger.error(
                "Failed to create catalog via SDK",
                catalog=catalog_name,
                error=error_msg,
            )

            # Provide specific error messages
            if "permission" in error_lower or "forbidden" in error_lower or "unauthorized" in error_lower:
                return {
                    "success": False,
                    "message": (
                        f"Permission denied: Service principal does not have permission to create catalog "
                        f"'{catalog_name}'. Please ensure the service principal has 'CREATE CATALOG' privilege."
                    ),
                    "created": False,
                }
            elif "storage" in error_lower or "location" in error_lower:
                return {
                    "success": False,
                    "message": (
                        f"Storage configuration error while creating catalog '{catalog_name}': {error_msg}. "
                        f"Please check the Unity Catalog metastore configuration and external location."
                    ),
                    "created": False,
                }
            elif "already exists" in error_lower:
                return {
                    "success": False,
                    "message": f"Catalog '{catalog_name}' already exists",
                    "created": False,
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to create catalog '{catalog_name}': {error_msg}",
                    "created": False,
                }

        # Catalog created successfully
        # When created by service principal, it becomes the owner with full privileges
        try:
            from dbrx_api.settings import Settings

            _settings = Settings()
            _client_id = _settings.client_id
        except Exception:
            _client_id = "(unknown)"
        logger.info(
            "Catalog created successfully, service principal is owner",
            catalog=catalog_name,
            service_principal=_client_id,
        )

        return {
            "success": True,
            "message": f"Catalog '{catalog_name}' created and ready",
            "created": True,
        }

    except Exception as e:
        logger.error(
            "Unexpected error in create_catalog",
            catalog=catalog_name,
            error=str(e),
        )
        return {
            "success": False,
            "message": f"Unexpected error creating catalog: {str(e)}",
            "created": False,
        }


def get_catalog(workspace_url: str, catalog_name: str) -> dict:
    """
    Get catalog information.

    Args:
        workspace_url: Databricks workspace URL
        catalog_name: Name of the catalog

    Returns:
        dict with 'exists' (bool), 'owner' (str), 'message' (str)
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return {"exists": False, "message": "Databricks SDK is not available"}

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        try:
            catalog = w_client.catalogs.get(name=catalog_name)
            logger.info(
                "Catalog found",
                catalog=catalog_name,
                owner=catalog.owner if catalog else None,
            )
            return {
                "exists": True,
                "owner": catalog.owner if catalog else None,
                "message": f"Catalog '{catalog_name}' exists",
            }
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg or "catalog_not_found" in error_msg:
                logger.info("Catalog not found", catalog=catalog_name)
                return {
                    "exists": False,
                    "message": f"Catalog '{catalog_name}' does not exist",
                }
            else:
                logger.error("Error checking catalog", catalog=catalog_name, error=str(e))
                return {
                    "exists": False,
                    "message": f"Error checking catalog '{catalog_name}': {str(e)}",
                }

    except Exception as e:
        logger.error(
            "Unexpected error in get_catalog",
            catalog=catalog_name,
            error=str(e),
        )
        return {
            "exists": False,
            "message": f"Unexpected error checking catalog: {str(e)}",
        }


def list_catalogs(workspace_url: str) -> list:
    """
    List all catalogs in the workspace.

    Args:
        workspace_url: Databricks workspace URL

    Returns:
        List of catalog dictionaries with 'name' and 'owner'
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return []

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        catalogs = []
        for catalog in w_client.catalogs.list():
            catalogs.append(
                {
                    "name": catalog.name,
                    "owner": catalog.owner if catalog.owner else None,
                }
            )

        logger.info("Listed catalogs", count=len(catalogs))
        return catalogs

    except Exception as e:
        logger.error("Error listing catalogs", error=str(e))
        return []


def delete_catalog(workspace_url: str, catalog_name: str) -> str | None:
    """
    Delete a Unity Catalog.

    Args:
        workspace_url: Databricks workspace URL
        catalog_name: Name of the catalog to delete

    Returns:
        None on success, error message string on failure
    """
    if not DATABRICKS_SDK_AVAILABLE:
        return "Databricks SDK is not available"

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        # Check if catalog exists first
        try:
            w_client.catalogs.get(name=catalog_name)
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg or "catalog_not_found" in error_msg:
                return f"Catalog '{catalog_name}' not found"
            else:
                return f"Error accessing catalog '{catalog_name}': {str(e)}"

        # Delete the catalog
        try:
            w_client.catalogs.delete(name=catalog_name, force=True)
            logger.info("Catalog deleted successfully", catalog=catalog_name)
            return None
        except Exception as delete_error:
            error_msg = str(delete_error).lower()
            if "permission" in error_msg or "forbidden" in error_msg:
                return f"Permission denied to delete catalog '{catalog_name}'"
            else:
                return f"Failed to delete catalog '{catalog_name}': {str(delete_error)}"

    except Exception as e:
        logger.error(
            "Unexpected error in delete_catalog",
            catalog=catalog_name,
            error=str(e),
        )
        return f"Unexpected error deleting catalog: {str(e)}"
