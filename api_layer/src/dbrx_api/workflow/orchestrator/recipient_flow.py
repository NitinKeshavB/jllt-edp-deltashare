"""
Unified recipient provisioning for workflow (strategy-agnostic).

- If recipient exists: compare params; if matching log "already matching", else update (D2D: description; D2O: IPs, revoke, token expiry, rotate, description).
- If recipient does not exist: create (with validations).
- On any failure: fail workflow with exact error and roll back all recipient changes to previous state.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4

from loguru import logger

from dbrx_api.dltshr.recipient import add_recipient_ip
from dbrx_api.dltshr.recipient import create_recipient_d2d
from dbrx_api.dltshr.recipient import create_recipient_d2o
from dbrx_api.dltshr.recipient import delete_recipient
from dbrx_api.dltshr.recipient import get_recipients
from dbrx_api.dltshr.recipient import revoke_recipient_ip
from dbrx_api.dltshr.recipient import rotate_recipient_token
from dbrx_api.dltshr.recipient import update_recipient_description
from dbrx_api.dltshr.recipient import update_recipient_expiration_time


def _ips_add_and_remove_from_config(
    recip_config: Dict[str, Any],
    current_ips: set,
) -> Tuple[set, set]:
    """
    Compute which IPs to add and which to remove. Idempotent and explicit:
    - Add: only IPs from recipient_ips_to_add that are not already in Databricks (current_ips).
    - Remove: only when recipient_ips_to_remove is explicitly set (non-empty); only revoke IPs
      that are both in that list and currently present in Databricks.
    We never remove IPs based on a "full list" (recipient_ips); only recipient_ips_to_remove.
    """
    add_list = recip_config.get("recipient_ips_to_add") or []
    remove_list = recip_config.get("recipient_ips_to_remove") or []
    ips_to_add = set(add_list) - current_ips
    if remove_list and len(remove_list) > 0:
        ips_to_remove = set(remove_list) & current_ips
    else:
        ips_to_remove = set()
    return ips_to_add, ips_to_remove


def _effective_ips_after_changes(
    recip_config: Dict[str, Any],
    current_ips: set,
) -> set:
    """Effective IP set after applying add/remove (for DB logging)."""
    ips_to_add, ips_to_remove = _ips_add_and_remove_from_config(recip_config, current_ips)
    return (current_ips | ips_to_add) - ips_to_remove


def _previous_state_d2d(existing: Any) -> Dict[str, Any]:
    """Capture previous state for D2D rollback (description only)."""
    return {
        "comment": (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else "",
    }


def _previous_state_d2o(existing: Any) -> Dict[str, Any]:
    """Capture previous state for D2O rollback (comment, IPs, expiration)."""
    ips = []
    if (
        hasattr(existing, "ip_access_list")
        and existing.ip_access_list
        and getattr(existing.ip_access_list, "allowed_ip_addresses", None)
    ):
        ips = list(existing.ip_access_list.allowed_ip_addresses)
    return {
        "comment": (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else "",
        "ip_list": ips,
        "expiration_time": getattr(existing, "expiration_time", None),
    }


def _restore_recipient_state(
    recipient_name: str,
    recipient_type: str,
    previous_state: Dict[str, Any],
    workspace_url: str,
) -> None:
    """Restore recipient to previous state (for rollback) using add_recipient_ip and revoke_recipient_ip."""
    try:
        if previous_state.get("comment") is not None:
            update_recipient_description(
                recipient_name=recipient_name,
                description=previous_state["comment"],
                dltshr_workspace_url=workspace_url,
            )
        if recipient_type == "D2O" and "ip_list" in previous_state:
            previous_ips = set(previous_state["ip_list"])
            current = get_recipients(recipient_name, workspace_url)
            current_ips = set()
            if current and getattr(current, "ip_access_list", None):
                current_ips = set(current.ip_access_list.allowed_ip_addresses or [])
            to_revoke = current_ips - previous_ips
            to_add = previous_ips - current_ips
            if to_revoke:
                revoke_recipient_ip(
                    recipient_name=recipient_name,
                    ip_access_list=list(to_revoke),
                    dltshr_workspace_url=workspace_url,
                )
            if to_add:
                add_recipient_ip(
                    recipient_name=recipient_name,
                    ip_access_list=list(to_add),
                    dltshr_workspace_url=workspace_url,
                )
        if previous_state.get("expiration_time") is not None:
            logger.warning(f"Rollback: expiration_time for {recipient_name} not restored (API uses days from now)")
    except Exception as e:
        logger.error(f"Rollback: failed to restore {recipient_name} to previous state: {e}")


def _rollback_recipients(rollback_list: List[tuple], workspace_url: str) -> None:
    """Roll back all recipient changes in reverse order."""
    for item in reversed(rollback_list):
        action = item[0]
        recipient_name = item[1]
        if action == "created":
            try:
                result = delete_recipient(recipient_name=recipient_name, dltshr_workspace_url=workspace_url)
                if result is not None and isinstance(result, str) and "error" in result.lower():
                    logger.error(f"Rollback delete recipient {recipient_name}: {result}")
                else:
                    logger.info(f"Rollback: deleted newly created recipient {recipient_name}")
            except Exception as e:
                logger.error(f"Rollback: failed to delete recipient {recipient_name}: {e}")
        elif action == "updated":
            recipient_type = item[2]
            previous_state = item[3]
            _restore_recipient_state(recipient_name, recipient_type, previous_state, workspace_url)
            logger.info(f"Rollback: restored recipient {recipient_name} to previous state")


def _apply_recipient_updates(
    recipient_name: str,
    recipient_type: str,
    recip_config: Dict[str, Any],
    existing: Any,
    workspace_url: str,
) -> None:
    """Apply updates to an existing recipient (D2D: description; D2O: description, IPs, token expiry, rotate)."""
    # Description (both types)
    new_description = (recip_config.get("description") or "").strip()
    current_comment = (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else ""
    if new_description != current_comment:
        result = update_recipient_description(recipient_name, new_description, workspace_url)
        if isinstance(result, str) and "error" in result.lower():
            raise RuntimeError(f"Failed to update description for {recipient_name}: {result}")

    if recipient_type != "D2O":
        return

    # D2O: token expiry and rotation
    token_expiry_days = recip_config.get("token_expiry", 0)
    token_rotation = recip_config.get("token_rotation", False)

    if token_expiry_days == 0 and token_rotation:
        result = rotate_recipient_token(
            recipient_name=recipient_name,
            dltshr_workspace_url=workspace_url,
            expire_in_seconds=0,
        )
        if isinstance(result, str):
            raise RuntimeError(f"Failed to rotate token for {recipient_name}: {result}")
    elif token_expiry_days > 0 and token_rotation:
        expire_seconds = token_expiry_days * 24 * 60 * 60
        result = rotate_recipient_token(
            recipient_name=recipient_name,
            dltshr_workspace_url=workspace_url,
            expire_in_seconds=expire_seconds,
        )
        if isinstance(result, str):
            raise RuntimeError(f"Failed to rotate token for {recipient_name}: {result}")
    elif token_expiry_days > 0:
        result = update_recipient_expiration_time(
            recipient_name=recipient_name,
            expiration_time=token_expiry_days,
            dltshr_workspace_url=workspace_url,
        )
        if isinstance(result, str) and "error" in result.lower():
            raise RuntimeError(f"Failed to set token expiry for {recipient_name}: {result}")

    # D2O: IPs - add only when not present in Databricks; remove only when recipient_ips_to_remove is explicitly set
    current_ips = set()
    if existing.ip_access_list and getattr(existing.ip_access_list, "allowed_ip_addresses", None):
        current_ips = set(existing.ip_access_list.allowed_ip_addresses)
    ips_to_add, ips_to_remove = _ips_add_and_remove_from_config(recip_config, current_ips)

    if ips_to_add:
        result = add_recipient_ip(
            recipient_name=recipient_name, ip_access_list=list(ips_to_add), dltshr_workspace_url=workspace_url
        )
        if isinstance(result, str):
            raise RuntimeError(f"Failed to add IPs to {recipient_name}: {result}")
    if ips_to_remove:
        result = revoke_recipient_ip(
            recipient_name=recipient_name,
            ip_access_list=list(ips_to_remove),
            dltshr_workspace_url=workspace_url,
        )
        if result is None or isinstance(result, str):
            raise RuntimeError(f"Failed to revoke IPs from {recipient_name}: {result}")


async def ensure_recipients(
    workspace_url: str,
    recipients_config: List[Dict[str, Any]],
    rollback_list: List[tuple],
    db_entries: List[Dict[str, Any]],
    created_resources: Optional[Dict[str, List]] = None,
) -> None:
    """
    Ensure all recipients exist with desired state (strategy-agnostic).
    Databricks operations only — no DB writes. Populates mutable rollback_list and db_entries.
    On failure, raises without rollback — the orchestrator handles all rollback.

    Raises:
        Exception: On first recipient create/update failure (orchestrator handles rollback).
    """
    if created_resources is None:
        created_resources = {"recipients": []}

    for recip_config in recipients_config:
        recipient_name = recip_config["name"]
        recipient_type = recip_config["type"]
        logger.info(f"Ensuring recipient: {recipient_name} ({recipient_type})")

        existing = get_recipients(recipient_name, workspace_url)

        if existing:
            # Recipient exists: validate immutable fields first
            if recipient_type == "D2D":
                # Validate that recipient_databricks_org hasn't changed (immutable field)
                current_org = getattr(existing, "data_recipient_global_metastore_id", None)
                new_org = recip_config.get("recipient_databricks_org") or recip_config.get(
                    "data_recipient_global_metastore_id"
                )
                # Only raise error if both values exist AND they differ (case-insensitive comparison)
                if new_org and current_org:
                    current_normalized = current_org.strip().lower()
                    new_normalized = new_org.strip().lower()
                    if current_normalized != new_normalized:
                        # recipient_databricks_org (data_recipient_global_metastore_id) is immutable —
                        # it can only be set at creation time and cannot be changed via the Databricks API.
                        # Log a warning and skip this field; proceed with other updates (e.g. description).
                        logger.warning(
                            f"Skipping 'recipient_databricks_org' change for D2D recipient '{recipient_name}': "
                            f"this field is immutable and cannot be updated after creation "
                            f"(current={current_org}, requested={new_org}). "
                            f"Proceeding with other updates (e.g. description) only."
                        )
                    else:
                        # Values match - log that we're ignoring it (no update needed)
                        logger.debug(
                            f"Ignoring recipient_databricks_org for '{recipient_name}': "
                            f"value matches existing ({current_org})"
                        )

            # Recipient exists: compare and update if needed
            current_comment = (
                (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else ""
            )
            new_description = (recip_config.get("description") or "").strip()
            description_matches = current_comment == new_description

            if recipient_type == "D2O":
                current_ips = set()
                if existing.ip_access_list and getattr(existing.ip_access_list, "allowed_ip_addresses", None):
                    current_ips = set(existing.ip_access_list.allowed_ip_addresses)
                ips_to_add, ips_to_remove = _ips_add_and_remove_from_config(recip_config, current_ips)
                ips_match = len(ips_to_add) == 0 and len(ips_to_remove) == 0
                token_expiry = recip_config.get("token_expiry")
                token_rotation = recip_config.get("token_rotation")
                token_unchanged = not token_expiry and not token_rotation
                all_match = description_matches and ips_match and token_unchanged
            else:
                all_match = description_matches

            if all_match:
                logger.info(f"Recipient parameters for '{recipient_name}' are already matching; no update needed")
                current_ips_list = (
                    list(existing.ip_access_list.allowed_ip_addresses)
                    if existing.ip_access_list and getattr(existing.ip_access_list, "allowed_ip_addresses", None)
                    else []
                )
                current_desc = (
                    (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else ""
                )
                # Use actual value from Databricks for recipient_databricks_org (immutable field)
                current_org = (
                    getattr(existing, "data_recipient_global_metastore_id", None) if recipient_type == "D2D" else None
                )
                db_entries.append(
                    {
                        "action": "matching",
                        "recipient_name": recipient_name,
                        "databricks_recipient_id": recipient_name,
                        "recipient_type": recipient_type,
                        "recipient_databricks_org": current_org,
                        "ip_access_list": current_ips_list if recipient_type == "D2O" else [],
                        "token_expiry_days": recip_config.get("token_expiry", 0),
                        "token_rotation_enabled": recip_config.get("token_rotation", False),
                        "description": current_desc,
                    }
                )
                created_resources["recipients"].append(f"{recipient_name} (already matching)")
                continue

            # Capture previous state for rollback, then apply updates
            if recipient_type == "D2D":
                previous_state = _previous_state_d2d(existing)
            else:
                previous_state = _previous_state_d2o(existing)

            _apply_recipient_updates(
                recipient_name=recipient_name,
                recipient_type=recipient_type,
                recip_config=recip_config,
                existing=existing,
                workspace_url=workspace_url,
            )
            rollback_list.append(("updated", recipient_name, recipient_type, previous_state))
            created_resources["recipients"].append(f"{recipient_name} (updated)")
            logger.success(f"Updated recipient: {recipient_name}")

            # Build db_entry with effective state after updates
            current_ips = set()
            if existing.ip_access_list and getattr(existing.ip_access_list, "allowed_ip_addresses", None):
                current_ips = set(existing.ip_access_list.allowed_ip_addresses)
            effective_ips = _effective_ips_after_changes(recip_config, current_ips) if recipient_type == "D2O" else []
            # Use actual value from Databricks for recipient_databricks_org (immutable field)
            current_org = (
                getattr(existing, "data_recipient_global_metastore_id", None) if recipient_type == "D2D" else None
            )
            db_entries.append(
                {
                    "action": "updated",
                    "recipient_name": recipient_name,
                    "databricks_recipient_id": recipient_name,
                    "recipient_type": recipient_type,
                    "recipient_databricks_org": current_org,
                    "ip_access_list": list(effective_ips) if recipient_type == "D2O" else [],
                    "token_expiry_days": recip_config.get("token_expiry", 0),
                    "token_rotation_enabled": recip_config.get("token_rotation", False),
                    "description": (recip_config.get("description") or "").strip(),
                }
            )
            continue

        # Recipient does not exist: create
        description_value = (recip_config.get("description") or "").strip()
        token_expiry_days = recip_config.get("token_expiry", 0)
        ip_list: List[str] = []

        if recipient_type == "D2D":
            recipient_identifier = recip_config.get("recipient_databricks_org") or recip_config.get(
                "data_recipient_global_metastore_id"
            )
            if not recipient_identifier:
                raise ValueError(
                    f"Recipient '{recipient_name}' (D2D) requires recipient_databricks_org or "
                    "data_recipient_global_metastore_id"
                )
            result = create_recipient_d2d(
                recipient_name=recipient_name,
                recipient_identifier=recipient_identifier,
                description=description_value,
                dltshr_workspace_url=workspace_url,
            )
        else:
            ip_list = recip_config.get("recipient_ips_to_add") or recip_config.get("recipient_ips") or []
            result = create_recipient_d2o(
                recipient_name=recipient_name,
                description=description_value,
                dltshr_workspace_url=workspace_url,
                ip_access_list=ip_list if ip_list else None,
            )

        if isinstance(result, str):
            raise RuntimeError(f"Failed to create recipient '{recipient_name}': {result}")

        rollback_list.append(("created", recipient_name, recipient_type, {}))

        # D2O: token expiry after create
        if recipient_type == "D2O" and token_expiry_days > 0:
            expiry_result = update_recipient_expiration_time(
                recipient_name=recipient_name,
                expiration_time=token_expiry_days,
                dltshr_workspace_url=workspace_url,
            )
            if isinstance(expiry_result, str) and "error" in expiry_result.lower():
                raise RuntimeError(f"Failed to set token expiry for '{recipient_name}': {expiry_result}")

        # D2O: ensure IPs applied (add any missing)
        if recipient_type == "D2O" and ip_list:
            actual_ips = (
                set(result.ip_access_list.allowed_ip_addresses)
                if hasattr(result, "ip_access_list")
                and result.ip_access_list
                and result.ip_access_list.allowed_ip_addresses
                else set()
            )
            expected_ips = set(ip_list)
            missing_ips = expected_ips - actual_ips
            if missing_ips:
                add_result = add_recipient_ip(
                    recipient_name=recipient_name,
                    ip_access_list=list(missing_ips),
                    dltshr_workspace_url=workspace_url,
                )
                if isinstance(add_result, str):
                    raise RuntimeError(f"Failed to add IP addresses to recipient '{recipient_name}': {add_result}")

        created_resources["recipients"].append(recipient_name)
        logger.success(f"Created recipient: {recipient_name}")

        recipient_id = uuid4()
        created_ips = ip_list if (recipient_type == "D2O" and ip_list) else []
        db_entries.append(
            {
                "action": "created",
                "recipient_id": recipient_id,
                "recipient_name": recipient_name,
                "databricks_recipient_id": result.name,
                "recipient_type": recipient_type,
                "recipient_databricks_org": recip_config.get("recipient_databricks_org")
                if recipient_type == "D2D"
                else None,
                "ip_access_list": created_ips,
                "token_expiry_days": token_expiry_days,
                "token_rotation_enabled": recip_config.get("token_rotation", False),
                "description": description_value,
            }
        )
