"""Module for managing Databricks recipients for Delta Sharing."""
from datetime import datetime
from datetime import timezone
from typing import List
from typing import Optional

from loguru import logger

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sharing import PermissionsChange
    from databricks.sdk.service.sharing import SharedDataObject
    from databricks.sdk.service.sharing import SharedDataObjectDataObjectType
    from databricks.sdk.service.sharing import SharedDataObjectUpdate
    from databricks.sdk.service.sharing import SharedDataObjectUpdateAction

except ImportError:
    logger.error("Failed to import required libraries")

import os

from dbrx_api.dbrx_auth.token_gen import get_auth_token

###################################


def list_shares_all(
    dltshr_workspace_url: str,
    max_results: Optional[int] = 100,
    prefix: Optional[str] = None,
) -> List:
    """List Delta Sharing shares with optional prefix filter.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        max_results: Maximum results per page (default: 100)
        prefix: Optional name prefix filter

    Returns:
        List of ShareInfo objects
    """
    logger.debug(
        "Listing shares from business logic",
        max_results=max_results,
        prefix=prefix,
        workspace_url=dltshr_workspace_url,
    )
    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        all_shares = []

        # List all shares using SDK - use list_shares method (not list)
        for share in w_client.shares.list_shares(max_results=max_results):
            if prefix:
                if prefix in str(share.name):
                    all_shares.append(share)
            else:
                all_shares.append(share)

        logger.debug("Shares listed successfully from business logic", count=len(all_shares), prefix=prefix)
        return all_shares
    except Exception as e:
        logger.error("Error listing shares from business logic", error=str(e), prefix=prefix, max_results=max_results)
        raise


def get_shares(share_name: str, dltshr_workspace_url: str):
    """Get share details by name.

    Args:
        share_name: Share name (case-sensitive)
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        ShareInfo object or None if not found
    """
    logger.debug("Getting share from business logic", share_name=share_name, workspace_url=dltshr_workspace_url)
    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get recipient by name
        response = w_client.shares.get(name=share_name)
        logger.debug("Share retrieved successfully from business logic", share_name=share_name, owner=response.owner)
        return response

    except Exception as e:
        logger.warning("Error retrieving share from business logic", share_name=share_name, error=str(e))
        return None


def create_share(
    dltshr_workspace_url: str,
    share_name: str,
    description: str,
    storage_root: Optional[str] = None,
):
    """Create a Delta Sharing share.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        share_name: Unique share name
        description: Share description
        storage_root: Optional storage root URL

    Returns:
        ShareInfo object on success, error message string on failure
    """
    # Get authentication token
    session_token = get_auth_token(datetime.now(timezone.utc))[0]

    # Create workspace client
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        # Create share
        print(f"DEBUG: Creating share {share_name}")
        print(f"DEBUG: - Description being passed: '{description}'")
        print(f"DEBUG: - Storage root: {storage_root}")
        response = w_client.shares.create(name=share_name, comment=description, storage_root=storage_root)
        print(
            f"DEBUG: Share created, comment in response: '{response.comment if hasattr(response, 'comment') else 'N/A'}'"
        )
    except Exception as e:
        err_msg = str(e)

        if "already exists" in err_msg or "AlreadyExists" in err_msg:
            print(f"✗ Share '{share_name}' already exists.")
            return f"Share already exists with name '{share_name}'"
        elif "PERMISSION_DENIED" in err_msg or "PermissionDenied" in err_msg:
            print(f"✗ Permission denied: User lacks CREATE_SHARE privilege or is not a metastore admin.")
            return (
                "Permission denied to create share. Caller must be a metastore admin or have CREATE_SHARE privilege."
            )
        elif "INVALID_PARAMETER_VALUE" in err_msg or "invalid" in err_msg.lower():
            print(f"✗ Invalid parameter: {err_msg}")
            return f"Invalid parameter in share creation: {err_msg}"
        elif "RESOURCE_DOES_NOT_EXIST" in err_msg:
            print(f"✗ Storage root location does not exist or is not accessible.")
            return "Storage root location does not exist or is not accessible"
        elif "INVALID_STATE" in err_msg:
            print(f"✗ Invalid state: {err_msg}")
            return f"Invalid state for share creation: {err_msg}"
        else:
            print(f"✗ Error creating share '{share_name}': {e}")
            raise

    return response


def add_data_object_to_share(
    dltshr_workspace_url: str,
    share_name: str,
    objects_to_add: [List[dict]],
):
    """Add data objects (tables, views, schemas) to a share.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        share_name: Share name
        objects_to_add: Dict with 'tables', 'views', 'schemas' lists

    Returns:
        ShareInfo object on success, error message string on failure
    """
    # Get authentication token
    session_token = get_auth_token(datetime.now(timezone.utc))[0]

    # Create workspace client
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        if objects_to_add is None or len(objects_to_add) == 0:
            return "No data objects provided to add to share."

        tables_to_add = objects_to_add.get("tables", [])
        views_to_add = objects_to_add.get("views", [])
        schemas_to_add = objects_to_add.get("schemas", [])

        if len(tables_to_add) == 0 and len(views_to_add) == 0 and len(schemas_to_add) == 0:
            return "No data objects provided to add to share."

        add_table_updates = []
        add_view_updates = []
        add_schema_updates = []

        if tables_to_add:
            add_table_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.ADD,
                    data_object=SharedDataObject(
                        name=table_name, data_object_type=SharedDataObjectDataObjectType.TABLE
                    ),
                )
                for table_name in tables_to_add
            ]

        if views_to_add:
            add_view_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.ADD,
                    data_object=SharedDataObject(name=view_name, data_object_type=SharedDataObjectDataObjectType.VIEW),
                )
                for view_name in views_to_add
            ]

        if schemas_to_add:
            # Extract schema names from tables and views being added
            table_schemas = set()
            view_schemas = set()

            for table_name in tables_to_add:
                # Extract schema from fully qualified name (catalog.schema.table)
                parts = table_name.split(".")
                if len(parts) >= 2:
                    schema_fqn = ".".join(parts[:-1])  # catalog.schema
                    table_schemas.add(schema_fqn)

            for view_name in views_to_add:
                parts = view_name.split(".")
                if len(parts) >= 2:
                    schema_fqn = ".".join(parts[:-1])
                    view_schemas.add(schema_fqn)

            # Check for conflicts
            conflicting_schemas = []
            for schema_name in schemas_to_add:
                if schema_name in table_schemas or schema_name in view_schemas:
                    conflicting_schemas.append(schema_name)

            if conflicting_schemas:
                conflict_msg = f"Cannot add schemas {conflicting_schemas} as individual tables/views from these schemas are already part of same request"
                print(f"✗ {conflict_msg}")
                return conflict_msg

            add_schema_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.ADD,
                    data_object=SharedDataObject(
                        name=schema_name, data_object_type=SharedDataObjectDataObjectType.SCHEMA
                    ),
                )
                for schema_name in schemas_to_add
            ]

        all_updates = add_table_updates + add_view_updates + add_schema_updates
        if all_updates:
            response = w_client.shares.update(name=share_name, updates=all_updates)
            return response

    except Exception as e:
        err_msg = str(e)

        if "ResourceAlreadyExists" in err_msg or "already exists" in err_msg:
            print(f"✗ Data object already exists in share- {share_name}: {err_msg}")
            return f"Data object already exists in share- {share_name}"
        elif "PERMISSION_DENIED" in err_msg or "PermissionDenied" in err_msg:
            print(f"✗ Permission denied: User lacks SELECT privilege on data objects or is not share owner")
            return "Permission denied. Share owner must have SELECT privilege on data objects."
        elif "User is not an owner of Share" in err_msg or "PERMISSION_DENIED" in err_msg:
            print(f"✗ Permission denied to add objects as User is not an owner of Share- {share_name}")
            return f"Permission denied to add objects as User is not an owner of Share- {share_name}"
        elif "RESOURCE_DOES_NOT_EXIST" in err_msg or "does not exist" in err_msg:
            print(f"✗ Data object not found in share- {share_name}: {err_msg}")
            return f"Data object not found in share- {share_name}: {err_msg}"
        elif (
            "databricks.sdk.errors.platform.InvalidParameterValue" in err_msg
            or "is a table and not a VIEW"
            or "is a VIEW and not a Table" in err_msg.lower()
        ):
            print(f"✗ Invalid parameter: {err_msg}")
            return f"Invalid parameter in data object : {err_msg}"
        else:
            print(f"✗ Error adding data objects to share- '{share_name}': {e}")
            raise


def revoke_data_object_from_share(
    dltshr_workspace_url: str,
    share_name: str,
    objects_to_revoke: [List[dict]],
):
    """Remove data objects (tables, views, schemas) from a share.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        share_name: Share name
        objects_to_revoke: Dict with 'tables', 'views', 'schemas' lists

    Returns:
        ShareInfo object on success, error message string on failure
    """
    # Get authentication token
    session_token = get_auth_token(datetime.now(timezone.utc))[0]

    # Create workspace client
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        if objects_to_revoke is None or len(objects_to_revoke) == 0:
            return "No data objects provided to revoke from share."

        tables_to_revoke = objects_to_revoke.get("tables", [])
        views_to_revoke = objects_to_revoke.get("views", [])
        schemas_to_revoke = objects_to_revoke.get("schemas", [])

        if len(tables_to_revoke) == 0 and len(views_to_revoke) == 0 and len(schemas_to_revoke) == 0:
            return "No data objects provided to revoke from share."

        add_table_updates = []
        add_view_updates = []
        add_schema_updates = []

        if tables_to_revoke:
            add_table_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.REMOVE,
                    data_object=SharedDataObject(
                        name=table_name, data_object_type=SharedDataObjectDataObjectType.TABLE
                    ),
                )
                for table_name in tables_to_revoke
            ]

        if views_to_revoke:
            add_view_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.REMOVE,
                    data_object=SharedDataObject(name=view_name, data_object_type=SharedDataObjectDataObjectType.VIEW),
                )
                for view_name in views_to_revoke
            ]

        if schemas_to_revoke:
            # Extract schema names from tables and views being revoked
            table_schemas = set()
            view_schemas = set()

            for table_name in tables_to_revoke:
                # Extract schema from fully qualified name (catalog.schema.table)
                parts = table_name.split(".")
                if len(parts) >= 2:
                    schema_fqn = ".".join(parts[:-1])  # catalog.schema
                    table_schemas.add(schema_fqn)

            for view_name in views_to_revoke:
                parts = view_name.split(".")
                if len(parts) >= 2:
                    schema_fqn = ".".join(parts[:-1])
                    view_schemas.add(schema_fqn)

            # Check for conflicts
            conflicting_schemas = []
            for schema_name in schemas_to_revoke:
                if schema_name in table_schemas or schema_name in view_schemas:
                    conflicting_schemas.append(schema_name)

            if conflicting_schemas:
                conflict_msg = f"Cannot remove schemas {conflicting_schemas} as individual tables/views from these schemas are already part of same request"
                print(f"✗ {conflict_msg}")
                return conflict_msg

            add_schema_updates = [
                SharedDataObjectUpdate(
                    action=SharedDataObjectUpdateAction.REMOVE,
                    data_object=SharedDataObject(
                        name=schema_name, data_object_type=SharedDataObjectDataObjectType.SCHEMA
                    ),
                )
                for schema_name in schemas_to_revoke
            ]

        all_updates = add_table_updates + add_view_updates + add_schema_updates
        if all_updates:
            response = w_client.shares.update(name=share_name, updates=all_updates)
            return response

    except Exception as e:
        err_msg = str(e)

        if "PERMISSION_DENIED" in err_msg or "PermissionDenied" in err_msg:
            print(f"✗ Permission denied: User lacks SELECT privilege on data objects or is not share owner")
            return "Permission denied. Share owner must have SELECT privilege on data objects."
        elif "User is not an owner of Share" in err_msg or "PERMISSION_DENIED" in err_msg:
            return (
                f"Permission denied to revoke dataobjects from share  as User is not an owner of Share- {share_name}"
            )
        elif "RESOURCE_DOES_NOT_EXIST" in err_msg or "does not exist" in err_msg:
            print(f"✗ Data object not found: {err_msg}")
            return f"Data object not found in share- {share_name}: {err_msg}"
        elif (
            "databricks.sdk.errors.platform.InvalidParameterValue" in err_msg
            or "is a table and not a VIEW"
            or "is a VIEW and not a Table" in err_msg.lower()
        ):
            print(f"✗ Invalid parameter: {err_msg}")
            return f"Invalid parameter in data object : {err_msg}"
        else:
            print(f"✗ Error adding data objects to share- '{share_name}': {e}")
            raise


def add_recipients_to_share(
    dltshr_workspace_url: str,
    share_name: str,
    recipient_name: str,
):
    """Grant recipient SELECT permission on a share.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        share_name: Share name
        recipient_name: Recipient name

    Returns:
        UpdateSharePermissionsResponse on success, error message string on failure
    """
    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        # Get share details to check ownership
        share_info = w_client.shares.get(name=share_name)

        # Get current user info to check ownership
        current_user = w_client.current_user.me()
        current_username = current_user.user_name

        # Check if current user is the owner of the share
        if share_info.owner != current_username:
            msg = f"Permission denied to add recipient. User is not an " f"owner of Share: {share_name}"
            print(f"✗ {msg}")
            return msg

        # Check if current user is the owner of the recipient
        try:
            recipient_info = w_client.recipients.get(name=recipient_name)
            if recipient_info.owner != current_username:
                msg = f"Permission denied to add recipient. User is not " f"an owner of Recipient: {recipient_name}"
                print(f"✗ {msg}")
                return msg
        except Exception as recipient_error:
            # If recipient doesn't exist, return error
            if "does not exist" in str(recipient_error).lower():
                msg = f"Recipient not found: {recipient_name}"
                print(f"✗ {msg}")
                return msg
            # For other errors, let it propagate
            raise

        # Check if recipient already has access
        perms = w_client.shares.share_permissions(name=share_name)
        if perms and hasattr(perms, "privilege_assignments"):
            for assignment in perms.privilege_assignments or []:
                if assignment.principal == recipient_name and assignment.privileges:
                    msg = f"Recipient {recipient_name} already has " f"access to share: {share_name}"
                    print(f"✗ {msg}")
                    return msg

        # Try to add recipient permissions
        response = w_client.shares.update_permissions(
            name=share_name,
            changes=[PermissionsChange(principal=recipient_name, add=["SELECT"])],
        )
        return response
    except Exception as e:
        error_msg = str(e)
        error_msg_lower = error_msg.lower()

        # Check for already exists/granted errors
        if (
            "already has" in error_msg_lower
            or "already exists" in error_msg_lower
            or "already been granted" in error_msg_lower
            or "already granted" in error_msg_lower
            or "RESOURCE_ALREADY_EXISTS" in error_msg
        ):
            msg = f"Recipient {recipient_name} already has access to " f"share: {share_name}"
            print(f"✗ {msg}")
            return msg
        elif "User is not an owner of Share" in error_msg or "PERMISSION_DENIED" in error_msg:
            msg = f"Permission denied to add recipient. User is not an " f"owner of Share: {share_name}"
            print(f"✗ {msg}")
            return msg
        elif "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_msg_lower:
            print(f"✗ Share or recipient not found: {error_msg}")
            return f"Share or recipient not found: {error_msg}"
        else:
            print(f"✗ Failed to add recipient {recipient_name} access to " f"share {share_name}: {error_msg}")
            raise


def remove_recipients_from_share(
    dltshr_workspace_url: str,
    share_name: str,
    recipient_name: str,
):
    """Revoke recipient SELECT permission from a share.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        share_name: Share name
        recipient_name: Recipient name

    Returns:
        UpdateSharePermissionsResponse on success, error message string on failure
    """
    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        # Get share details to check ownership
        try:
            share_info = w_client.shares.get(name=share_name)
        except Exception as share_error:
            # If share doesn't exist, return error
            if "does not exist" in str(share_error).lower():
                msg = f"Share not found: {share_name}"
                print(f"✗ {msg}")
                return msg
            # For other errors, let it propagate
            raise

        # Get current user info to check ownership
        current_user = w_client.current_user.me()
        current_username = current_user.user_name

        # Check if current user is the owner of the share
        if share_info.owner != current_username:
            msg = f"Permission denied to remove recipient. User is not " f"an owner of Share: {share_name}"
            print(f"✗ {msg}")
            return msg

        # Check if current user is the owner of the recipient
        try:
            recipient_info = w_client.recipients.get(name=recipient_name)
            if recipient_info.owner != current_username:
                msg = f"Permission denied to remove recipient. User is not " f"an owner of Recipient: {recipient_name}"
                print(f"✗ {msg}")
                return msg
        except Exception as recipient_error:
            # If recipient doesn't exist, return error
            if "does not exist" in str(recipient_error).lower():
                msg = f"Recipient not found: {recipient_name}"
                print(f"✗ {msg}")
                return msg
            # For other errors, let it propagate
            raise

        # Check if recipient has access to the share
        perms = w_client.shares.share_permissions(name=share_name)
        recipient_has_access = False
        if perms and hasattr(perms, "privilege_assignments"):
            for assignment in perms.privilege_assignments or []:
                if assignment.principal == recipient_name:
                    recipient_has_access = True
                    break

        if not recipient_has_access:
            msg = f"Recipient {recipient_name} does not have access to " f"share: {share_name}"
            print(f"✗ {msg}")
            return msg

        # Remove recipient permissions
        response = w_client.shares.update_permissions(
            name=share_name,
            changes=[PermissionsChange(principal=recipient_name, remove=["SELECT"])],
        )
        return response
    except Exception as e:
        error_msg = str(e)
        error_msg_lower = error_msg.lower()
        if "User is not an owner of Share" in error_msg or "PERMISSION_DENIED" in error_msg:
            print(f"✗ Permission denied to remove recipient: {share_name}")
            return f"Permission denied to remove recipient. User is not an " f"owner of Share: {share_name}"
        elif "does not exist" in error_msg_lower:
            print(f"✗ Share or recipient not found: {error_msg}")
            return f"Share or recipient not found: {error_msg}"
        else:
            print(f"✗ Failed to remove recipient {recipient_name} access " f"to share {share_name}: {str(e)}")
            raise


def delete_share(
    share_name: str,
    dltshr_workspace_url: str,
):
    """Permanently delete a share.

    Args:
        share_name: Share name
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        None on success, error message string on failure
    """
    # Validate parameters

    try:
        session_token = get_auth_token(datetime.now(timezone.utc))[0]
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Delete share
        w_client.shares.delete(name=share_name)
        return None

    except Exception as ex:
        error_msg = str(ex)
        error_msg_lower = error_msg.lower()
        if (
            "User is not an owner of Share" in error_msg
            or "PERMISSION_DENIED" in error_msg
            or "PERMISSION DENIED" in error_msg
        ):
            print(f"✗ Permission denied to delete share: {share_name}")
            return "User is not an owner of Share"
        elif "RESOURCE_DOES_NOT_EXIST" in error_msg or "does not exist" in error_msg_lower:
            print(f"✗ Share not found: {share_name}")
            return f"Share not found: {share_name}"
        else:
            print(f"✗ Unexpected error deleting share: {ex}")
            raise


if __name__ == "__main__":
    dltshr_workspace_url = os.getenv("DLTSHR_WORKSPACE_URL")
    shares = add_recipients_to_share(
        dltshr_workspace_url=dltshr_workspace_url,
        share_name="share_1765402291",
        recipient_name="gmail_test",
    )
    print(shares)
