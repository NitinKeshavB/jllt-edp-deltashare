"""Module for managing Databricks recipients for Delta Sharing."""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import List
from typing import Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sharing import AuthenticationType
    from databricks.sdk.service.sharing import IpAccessList

except ImportError:
    print("failed to import libraries")

from dbrx_api.dbrx_auth.token_gen import get_auth_token

# DLTSHR_WORKSPACE_URL = os.getenv("DLTSHR_WORKSPACE_URL")


def list_recipients(
    dltshr_workspace_url: str,
    max_results: Optional[int] = 100,
    prefix: Optional[str] = None,
) -> list:
    """List all Delta Sharing recipients with optional prefix filter.

    Args:
        dltshr_workspace_url: Databricks workspace URL
        max_results: Maximum results per page (default: 100)
        prefix: Optional name prefix to filter recipients

    Returns:
        List of recipient objects
    """
    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    all_recipients = []

    # The list() method returns an iterator that automatically handles
    # pagination
    for recipient in w_client.recipients.list(max_results=max_results):
        if prefix:
            if prefix in str(recipient.name):
                all_recipients.append(recipient)
        else:
            all_recipients.append(recipient)

    return all_recipients


def get_recipients(recipient_name: str, dltshr_workspace_url: str):
    """Get recipient details by name.

    Args:
        recipient_name: Name of the recipient (case-sensitive)
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        RecipientInfo object or None if not found
    """
    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get recipient by name
        response = w_client.recipients.get(name=recipient_name)
        return response

    except Exception as e:
        print(f"✗ Error retrieving recipient '{recipient_name}': {e}")
        return None


def create_recipient_d2d(
    recipient_name: str,
    recipient_identifier: str,
    description: str,
    dltshr_workspace_url: str,
    sharing_code: Optional[str] = None,
):
    """Create a Databricks-to-Databricks recipient with DATABRICKS authentication.

    Args:
        recipient_name: Unique recipient name
        recipient_identifier: Metastore ID (format: cloud:region:uuid)
        description: Recipient description
        dltshr_workspace_url: Databricks workspace URL
        sharing_code: Optional sharing code

    Returns:
        RecipientInfo object or error message string

    Note:
        D2D recipients do NOT support IP access lists.
    """
    # Get authentication token
    session_token = get_auth_token(datetime.now(timezone.utc))[0]

    # Create workspace client
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    try:
        # Create D2D recipient (no ip_access_list for D2D type)
        response = w_client.recipients.create(
            name=recipient_name,
            data_recipient_global_metastore_id=recipient_identifier,
            comment=description,
            authentication_type=AuthenticationType.DATABRICKS,
            sharing_code=sharing_code if sharing_code else None,
        )
    except Exception as e:
        err_msg = str(e)
        if "Cannot resolve target shard" in err_msg:
            print(
                f"✗ Error: Invalid recipient_identifier "
                f"'{recipient_identifier}'. Please verify the metastore ID format."
            )
            return f"Invalid recipient_identifier. please verify {recipient_identifier}"
        elif "There is already a Recipient object" in err_msg:
            print(f"✗ Recipient '{recipient_name}' already exists.")
            return f"Recipient already exists with same sharing identifier {recipient_identifier}"
        else:
            print(f"✗ Error creating D2D recipient '{recipient_name}': {e}")
            raise

    return response


def create_recipient_d2o(
    recipient_name: str,
    description: str,
    dltshr_workspace_url: str,
    ip_access_list: Optional[List[str]] = None,
):
    """Create a Databricks-to-Open recipient with TOKEN authentication.

    Args:
        recipient_name: Unique recipient name
        description: Recipient description
        dltshr_workspace_url: Databricks workspace URL
        ip_access_list: Optional list of IP addresses/CIDR blocks

    Returns:
        RecipientInfo object with activation URL and tokens
    """
    # Process IP access list
    ip_access = None
    if ip_access_list:
        # Strip whitespace from each IP address
        cleaned_ips = [ip.strip() for ip in ip_access_list if ip.strip()]
        print(f"DEBUG: Received ip_access_list for {recipient_name}: {ip_access_list}")
        print(f"DEBUG: Cleaned IPs: {cleaned_ips}")

        if cleaned_ips:
            ip_access = IpAccessList(allowed_ip_addresses=cleaned_ips)
            print(f"DEBUG: Created IpAccessList object with {len(cleaned_ips)} IPs")
    else:
        print(f"DEBUG: No ip_access_list provided for {recipient_name}")

    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Create TOKEN recipient with optional IP access list
        print(f"DEBUG: Creating recipient {recipient_name}")
        print(f"DEBUG: - Description being passed: '{description}'")
        print(f"DEBUG: - IP access: {'None' if ip_access is None else f'{len(ip_access.allowed_ip_addresses)} IPs'}")
        response = w_client.recipients.create(
            name=recipient_name,
            comment=description,
            authentication_type=AuthenticationType.TOKEN,
            ip_access_list=ip_access,
        )
        print(f"DEBUG: Recipient created, comment in response: '{response.comment if hasattr(response, 'comment') else 'N/A'}'")

        return response
    except Exception as ex:
        err_msg = str(ex)
        if "There is already a Recipient object" in err_msg or "already exists" in err_msg.lower():
            print(f"✗ Recipient '{recipient_name}' already exists.")
            return f"Recipient '{recipient_name}' already exists"
        else:
            print(f"✗ Unexpected error creating recipient: {ex}")
            raise


def rotate_recipient_token(
    recipient_name: str,
    dltshr_workspace_url: str,
    expire_in_seconds: int = 0,
):
    """Rotate token for TOKEN-based recipient.

    Args:
        recipient_name: Recipient name
        dltshr_workspace_url: Databricks workspace URL
        expire_in_seconds: Seconds until old token expires (0=immediate)

    Returns:
        RecipientInfo with new token or error message string
    """
    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Rotate token for recipient
        response = w_client.recipients.rotate_token(
            name=recipient_name,
            existing_token_expire_in_seconds=expire_in_seconds,
        )

        return response

    except Exception as ex:
        message = str(ex)
        if "Cannot extend the token expiration time" in message:
            print(
                f"✗ Error: Cannot set expire_in_seconds " f"to {expire_in_seconds} for recipient '{recipient_name}'."
            )
            return f"Cannot extend the token expiration time to {expire_in_seconds} seconds as per token policy"
        elif "There are already two activated tokens for recipient" in message:
            print(f"✗ Error: Recipient '{recipient_name}' " f"already has maximum active tokens.")
            return "Recipient already has maximum number of active tokens"
        elif "User is not an owner of Recipient" in message:
            print("✗ Permission denied to rotate token of recipient as user is not the owner")
            return "Permission denied to rotate token of recipient as user is not the owner of the recipient"
        elif "non-TOKEN authentication type" in message:
            print(f"✗ Error: Recipient '{recipient_name}' is not a TOKEN type recipient.")
            return f"Recipient '{recipient_name}' is non-TOKEN type recipient (databricks to databricks) hence cannot rotate token"
        else:
            print(f"✗ Unexpected error rotating token: {ex}")
            raise


def add_recipient_ip(
    recipient_name: str,
    ip_access_list: List[str],
    dltshr_workspace_url: str,
):
    """Add IP addresses to TOKEN recipient's access list.

    Args:
        recipient_name: Recipient name
        ip_access_list: List of IP addresses/CIDR blocks to add
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        Updated RecipientInfo or None if failed

    Note:
        Merges with existing IPs. Only works with TOKEN recipients.
    """
    # Process new IP addresses
    cleaned_ips = [ip.strip() for ip in ip_access_list if ip.strip()]

    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get current recipient to retrieve existing IPs
        recipient = w_client.recipients.get(name=recipient_name)

        # Merge with existing IPs if they exist
        if recipient.ip_access_list and recipient.ip_access_list.allowed_ip_addresses:
            existing_ips = recipient.ip_access_list.allowed_ip_addresses
            # Combine and deduplicate IPs
            all_ips = list(set(cleaned_ips + existing_ips))
        else:
            all_ips = cleaned_ips

        # Create IP access list object
        ip_access = IpAccessList(allowed_ip_addresses=all_ips) if all_ips else None

        # Update recipient IP access list
        response = w_client.recipients.update(name=recipient_name, ip_access_list=ip_access)

        return response
    except Exception as ex:
        message = str(ex)
        if "User is not an owner of Recipient" in message:
            print("✗ Permission denied to add IP to recipient as user is not the owner")
            return "Permission denied to add IP to recipient as user is not the owner of the recipient"
        else:
            print(f"✗ Unexpected error updating IP access list: {ex}")
            raise


def revoke_recipient_ip(
    recipient_name: str,
    ip_access_list: List[str],
    dltshr_workspace_url: str,
):
    """Remove IP addresses from TOKEN recipient's access list.

    Args:
        recipient_name: Recipient name
        ip_access_list: List of IP addresses/CIDR blocks to remove
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        Updated RecipientInfo or None if failed

    Note:
        Only works with TOKEN recipients. IPs not in current list are ignored.
    """
    # Validate required parameters

    if not isinstance(ip_access_list, list):
        print("✗ Error: ip_access_list must be a list of IP addresses")
        return None

    # Process IP addresses to remove
    ips_to_remove = set(ip.strip() for ip in ip_access_list if ip.strip())

    if not ips_to_remove:
        print("✗ Error: No valid IP addresses provided to remove")
        return None

    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Get current recipient details
        recipient = w_client.recipients.get(name=recipient_name)

        # Check if recipient has IP restrictions
        if not recipient.ip_access_list or not recipient.ip_access_list.allowed_ip_addresses:
            print(f"✗ Recipient '{recipient_name}' has no IP " "restrictions to remove")
            return None

        existing_ips = set(recipient.ip_access_list.allowed_ip_addresses)

        # Remove specified IPs from existing list
        remaining_ips = list(existing_ips - ips_to_remove)

        # Check what was actually removed
        actually_removed = existing_ips & ips_to_remove
        not_found = ips_to_remove - existing_ips

        if not actually_removed:
            print("✗ None of the specified IPs were found in the access list")
            if not_found:
                print(f"  IPs not found: {', '.join(not_found)}")
            return None

        if not_found:
            print(f"  Note: {len(not_found)} IP(s) were not found and skipped")

        if not remaining_ips:
            print("  Warning: All IPs removed - recipient will have no " "IP restrictions")

        # Create IP access list object (empty list if no IPs remaining)
        ip_access = IpAccessList(allowed_ip_addresses=remaining_ips)

        # Update recipient IP access list
        response = w_client.recipients.update(name=recipient_name, ip_access_list=ip_access)

        return response

    except Exception as ex:
        message = str(ex)
        if "User is not an owner of Recipient" in message:
            print("✗ Permission denied to revoke IP to recipient as user is not the owner")
            return "Permission denied to revoke IP to recipient as user is not the owner of the recipient"
        else:
            print(f"✗ Unexpected error removing IP addresses: {ex}")
        raise


def update_recipient_description(
    recipient_name: str,
    description: str,
    dltshr_workspace_url: str,
):
    """Update recipient description.

    Args:
        recipient_name: Recipient name
        description: New description text
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        Updated RecipientInfo or error message string
    """
    # Validate parameters

    if not description or not description.strip():
        print("✗ Error: description cannot be empty")
        return None

    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Update recipient description
        response = w_client.recipients.update(name=recipient_name, comment=description)

        return response

    except Exception as ex:
        error_msg = str(ex)
        if "User is not an owner of Recipient" in error_msg:
            print(f"✗ Permission denied to update description of recipient as user is not the owner")
            return "Permission denied to update description of recipient as user is not the owner"
        else:
            print(f"✗ Unexpected error updating recipient description: {ex}")
            raise


def update_recipient_expiration_time(
    recipient_name: str,
    expiration_time: int,
    dltshr_workspace_url: str,
):
    """Update recipient expiration time.

    Args:
        recipient_name: Recipient name
        expiration_time: Days from now until expiration
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        Updated RecipientInfo or error message string
    """
    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Convert days to epoch milliseconds
        expiration_datetime = datetime.now(timezone.utc) + timedelta(days=expiration_time)
        expiration_epoch_ms = int(expiration_datetime.timestamp() * 1000)

        # Update recipient expiration time
        response = w_client.recipients.update(name=recipient_name, expiration_time=expiration_epoch_ms)

    except Exception as ex:
        error_msg = str(ex)
        if "User is not an owner of Recipient" in error_msg:
            print(f"✗ Permission denied to update expiration time of recipient as user is not the owner")
            return "Permission denied to update expiration time of recipient as user is not the owner"
        else:
            print(f"✗ Unexpected error updating recipient description: {ex}")
            raise

    return response


def delete_recipient(
    recipient_name: str,
    dltshr_workspace_url: str,
):
    """Delete recipient permanently.

    Args:
        recipient_name: Recipient name
        dltshr_workspace_url: Databricks workspace URL

    Returns:
        None on success or error message string
    """
    # Validate parameters
    if not recipient_name or not recipient_name.strip():
        print("✗ Error: recipient_name cannot be empty")
        return None

    try:
        # Get authentication token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create workspace client
        w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)

        # Delete recipient
        response = w_client.recipients.delete(name=recipient_name)

        return response

    except Exception as ex:
        error_msg = str(ex)
        if "User is not an owner of Recipient" in error_msg:
            print(f"✗ Permission denied to delete recipient: {recipient_name}")
            return "User is not an owner of Recipient"
        if "Unauthorized access" in error_msg:
            print(f"✗ Permission denied to delete recipient: {recipient_name}")
            return "User is not an owner of Recipient"
        else:
            print(f"✗ Unexpected error deleting recipient: {ex}")
            raise
