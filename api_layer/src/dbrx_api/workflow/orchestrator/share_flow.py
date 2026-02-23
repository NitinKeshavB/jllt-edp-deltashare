"""
Unified share provisioning for workflow (strategy-agnostic).

- If share exists: update objects and recipients to match config; record for rollback.
- If share does not exist: create share, add objects, add recipients; record for rollback.
- On any failure: roll back all share changes and raise with exact error message.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4

from loguru import logger

from dbrx_api.dltshr.share import add_data_object_to_share
from dbrx_api.dltshr.share import add_recipients_to_share
from dbrx_api.dltshr.share import create_share
from dbrx_api.dltshr.share import delete_share
from dbrx_api.dltshr.share import get_share_objects
from dbrx_api.dltshr.share import get_share_recipients
from dbrx_api.dltshr.share import get_shares
from dbrx_api.dltshr.share import remove_recipients_from_share
from dbrx_api.dltshr.share import revoke_data_object_from_share
from dbrx_api.dltshr.share import update_share_description


def _assets_to_objects_dict(share_assets: List[str]) -> Dict[str, List[str]]:
    """Build objects_to_add/objects_to_revoke dict from share_assets (tables vs schemas)."""
    tables: List[str] = []
    schemas: List[str] = []
    for asset in share_assets or []:
        parts = asset.split(".")
        if len(parts) >= 3:
            tables.append(asset)
        else:
            schemas.append(asset)
    return {"tables": tables, "views": [], "schemas": schemas}


def _safe_rollback_op(fn: Any, success_msg: str, warn_ok: str = "") -> None:
    """Call fn(), check for error strings, log result. Never raises."""
    try:
        result = fn()
        if isinstance(result, str) and (not warn_ok or warn_ok.lower() not in result.lower()):
            logger.warning(f"Rollback warning: {result}")
        else:
            logger.info(success_msg)
    except Exception as exc:
        logger.error(f"Rollback error: {exc}")


def _has_objects(obj_dict: Dict[str, List[str]]) -> bool:
    return bool(obj_dict and (obj_dict.get("tables") or obj_dict.get("views") or obj_dict.get("schemas")))


def _rollback_share_update(
    share_name: str,
    objs_added: Dict[str, List[str]],
    objs_removed: Dict[str, List[str]],
    rec_added: List[str],
    rec_removed: List[str],
    workspace_url: str,
) -> None:
    """Undo a single share update. Each operation is independent of the others."""
    if _has_objects(objs_added):
        _safe_rollback_op(
            lambda: revoke_data_object_from_share(
                dltshr_workspace_url=workspace_url, share_name=share_name, objects_to_revoke=objs_added
            ),
            f"Rollback: revoked added objects from share '{share_name}'",
        )
    if _has_objects(objs_removed):
        _safe_rollback_op(
            lambda: add_data_object_to_share(
                dltshr_workspace_url=workspace_url, share_name=share_name, objects_to_add=objs_removed
            ),
            f"Rollback: re-added removed objects to share '{share_name}'",
            warn_ok="already",
        )
    for rec in rec_added or []:
        _safe_rollback_op(
            lambda r=rec: remove_recipients_from_share(
                dltshr_workspace_url=workspace_url, share_name=share_name, recipient_name=r
            ),
            f"Rollback: removed recipient '{rec}' from share '{share_name}'",
            warn_ok="not found",
        )
    for rec in rec_removed or []:
        _safe_rollback_op(
            lambda r=rec: add_recipients_to_share(
                dltshr_workspace_url=workspace_url, share_name=share_name, recipient_name=r
            ),
            f"Rollback: re-added recipient '{rec}' to share '{share_name}'",
            warn_ok="already",
        )


def _rollback_shares(
    rollback_list: List[Tuple[str, ...]],
    workspace_url: str,
) -> None:
    """Roll back share changes in reverse order."""
    for item in reversed(rollback_list):
        action = item[0]
        share_name = item[1]
        if action == "created":
            try:
                delete_share(dltshr_workspace_url=workspace_url, share_name=share_name)
                logger.info(f"Rollback: deleted newly created share {share_name}")
            except Exception as e:
                logger.error(f"Rollback: failed to delete share {share_name}: {e}")
        elif action == "updated":
            _rollback_share_update(
                share_name=share_name,
                objs_added=item[2],
                objs_removed=item[3],
                rec_added=item[4],
                rec_removed=item[5],
                workspace_url=workspace_url,
            )


async def ensure_shares(
    workspace_url: str,
    shares_config: List[Dict[str, Any]],
    rollback_list: List[Tuple[str, ...]],
    db_entries: List[Dict[str, Any]],
    created_resources: Optional[Dict[str, List]] = None,
    removed_assets_per_share: Optional[List[Dict[str, Any]]] = None,
    added_assets_per_share: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Ensure all shares exist with desired objects and recipients (strategy-agnostic).
    Databricks operations only — no DB writes. Populates mutable rollback_list and db_entries.
    On failure, raises without rollback — the orchestrator handles all rollback.

    Raises:
        Exception: On first share create/update failure (orchestrator handles rollback).
    """
    if created_resources is None:
        created_resources = {"shares": []}

    for share_config in shares_config:
        share_name = share_config["name"]

        # Share Asset management - support both declarative and explicit approaches
        # Approach 1 (Declarative): Use 'share_assets' field as complete desired state
        # Approach 2 (Explicit): Use 'share_assets_to_add' and 'share_assets_to_remove' for incremental changes
        share_assets_declarative = share_config.get("share_assets", [])
        share_assets_to_add_explicit = share_config.get("share_assets_to_add", [])
        share_assets_to_remove_explicit = share_config.get("share_assets_to_remove", [])

        # Determine which approach is being used for share assets
        if share_assets_declarative:
            # Declarative approach: use provided list as complete desired state
            desired_share_assets = list(share_assets_declarative)
            logger.debug(
                f"Share {share_name}: Using declarative share_assets (complete state): {desired_share_assets}"
            )
        elif share_assets_to_add_explicit or share_assets_to_remove_explicit:
            # Explicit approach: compute based on current + add - remove
            # For NEW strategy on non-existent share, current = empty set
            current_assets_list = []
            existing_check = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)
            if existing_check:
                current_objects = get_share_objects(share_name=share_name, dltshr_workspace_url=workspace_url)
                current_assets_list = (
                    current_objects.get("tables", [])
                    + current_objects.get("views", [])
                    + current_objects.get("schemas", [])
                )

            current_set = set(current_assets_list)
            to_add_set = set(share_assets_to_add_explicit)
            to_remove_set = set(share_assets_to_remove_explicit)

            desired_share_assets = list((current_set | to_add_set) - to_remove_set)
            logger.debug(
                f"Share {share_name}: Using explicit share_assets. "
                f"Current={current_set}, Add={to_add_set}, Remove={to_remove_set}, "
                f"Desired={desired_share_assets}"
            )
        else:
            # No share asset management specified - keep existing or create with none
            desired_share_assets = []
            logger.debug(f"Share {share_name}: No share_assets specified, using empty list")

        # Recipient management - support both declarative and explicit approaches
        # Approach 1 (Declarative): Use 'recipients' field as complete desired state
        # Approach 2 (Explicit): Use 'recipients_to_add' and 'recipients_to_remove' for incremental changes
        recipients_declarative = share_config.get("recipients", [])
        recipients_to_add_explicit = share_config.get("recipients_to_add", [])
        recipients_to_remove_explicit = share_config.get("recipients_to_remove", [])

        # Determine which approach is being used and compute desired recipients
        if recipients_declarative:
            # Declarative approach: use provided list as complete desired state
            desired_recipients = list(recipients_declarative)
            logger.debug(f"Share {share_name}: Using declarative recipients (complete state): {desired_recipients}")
        elif recipients_to_add_explicit or recipients_to_remove_explicit:
            # Explicit approach: compute based on current + add - remove
            # For NEW strategy on non-existent share, current = empty set
            current_recipients_list = []
            existing_check = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)
            if existing_check:
                current_recipients_obj = get_share_recipients(
                    share_name=share_name, dltshr_workspace_url=workspace_url
                )
                current_recipients_list = current_recipients_obj if isinstance(current_recipients_obj, list) else []

            current_set = set(current_recipients_list)
            to_add_set = set(recipients_to_add_explicit)
            to_remove_set = set(recipients_to_remove_explicit)

            desired_recipients = list((current_set | to_add_set) - to_remove_set)
            logger.debug(
                f"Share {share_name}: Using explicit recipients. "
                f"Current={current_set}, Add={to_add_set}, Remove={to_remove_set}, "
                f"Desired={desired_recipients}"
            )
        else:
            # No recipient management specified - keep existing or create with none
            desired_recipients = []
            logger.debug(f"Share {share_name}: No recipients specified, using empty list")

        delta_share_meta = share_config.get("delta_share") or {}
        ext_catalog_name = delta_share_meta.get("ext_catalog_name") or ""
        ext_schema_name = delta_share_meta.get("ext_schema_name") or ""
        prefix_assetname = delta_share_meta.get("prefix_assetname") or ""
        share_tags = delta_share_meta.get("tags") or share_config.get("tags") or []
        desc = share_config.get("description") or share_config.get("comment", "")

        logger.info(f"Ensuring share: {share_name}")

        existing = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)

        if existing is None:
            # Share does not exist: create, add objects, add recipients (or treat "already exists" as update)
            result = create_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                description=desc,
            )
            if isinstance(result, str):
                if "already exists" in result.lower() or "already present" in result.lower():
                    logger.warning(f"Share {share_name} already exists, treating as update")
                    existing = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)
                    if existing is None:
                        raise RuntimeError(f"Share {share_name} reported as existing but get_shares returned None")
                else:
                    raise RuntimeError(f"Failed to create share {share_name}: {result}")
            if existing is None:
                # We actually created the share
                rollback_list.append(("created", share_name))
                created_resources["shares"].append(share_name)
                logger.success(f"Created share: {share_name}")

                objects_dict = _assets_to_objects_dict(desired_share_assets)
                if objects_dict.get("tables") or objects_dict.get("schemas"):
                    add_result = add_data_object_to_share(
                        dltshr_workspace_url=workspace_url,
                        share_name=share_name,
                        objects_to_add=objects_dict,
                    )
                    if isinstance(add_result, str) and "already" not in add_result.lower():
                        raise RuntimeError(f"Failed to add data objects to share {share_name}: {add_result}")
                for recipient_name in desired_recipients:
                    add_rec_result = add_recipients_to_share(
                        dltshr_workspace_url=workspace_url,
                        share_name=share_name,
                        recipient_name=recipient_name,
                    )
                    if isinstance(add_rec_result, str) and "already" not in add_rec_result.lower():
                        raise RuntimeError(
                            f"Failed to add recipient {recipient_name} to share {share_name}: " f"{add_rec_result}"
                        )

                share_id = uuid4()
                db_entries.append(
                    {
                        "action": "created",
                        "share_id": share_id,
                        "share_name": share_name,
                        "databricks_share_id": result.name if hasattr(result, "name") else share_name,
                        "description": desc,
                        "storage_root": "",
                        "share_assets": desired_share_assets,
                        "recipients_attached": desired_recipients,
                        "ext_catalog_name": ext_catalog_name,
                        "ext_schema_name": ext_schema_name,
                        "prefix_assetname": prefix_assetname,
                        "share_tags": share_tags,
                    }
                )
                # All assets on a newly created share are "added" — track for pipeline check
                if desired_share_assets and added_assets_per_share is not None:
                    added_assets_per_share.append(
                        {
                            "share_name": share_name,
                            "added_assets": list(desired_share_assets),
                            "ext_catalog_name": ext_catalog_name,
                            "ext_schema_name": ext_schema_name,
                        }
                    )
                continue

        # Share exists: update objects and recipients to match config
        current_objects = get_share_objects(share_name=share_name, dltshr_workspace_url=workspace_url)
        current_recipients = get_share_recipients(share_name=share_name, dltshr_workspace_url=workspace_url)

        desired_objects = _assets_to_objects_dict(desired_share_assets)
        current_tables = set(current_objects.get("tables", []))
        current_views = set(current_objects.get("views", []))
        current_schemas = set(current_objects.get("schemas", []))
        desired_tables = set(desired_objects.get("tables", []))
        desired_views = set(desired_objects.get("views", []))
        desired_schemas = set(desired_objects.get("schemas", []))

        to_add_tables = list(desired_tables - current_tables)
        to_remove_tables = list(current_tables - desired_tables)
        to_add_views = list(desired_views - current_views)
        to_remove_views = list(current_views - desired_views)
        to_add_schemas = list(desired_schemas - current_schemas)
        to_remove_schemas = list(current_schemas - desired_schemas)

        objects_added = {
            "tables": to_add_tables,
            "views": to_add_views,
            "schemas": to_add_schemas,
        }
        objects_removed = {
            "tables": to_remove_tables,
            "views": to_remove_views,
            "schemas": to_remove_schemas,
        }
        current_rec_set = set(current_recipients)
        desired_rec_set = set(desired_recipients)
        recipients_added = list(desired_rec_set - current_rec_set)
        recipients_removed = list(current_rec_set - desired_rec_set)

        if not (
            to_add_tables
            or to_remove_tables
            or to_add_views
            or to_remove_views
            or to_add_schemas
            or to_remove_schemas
            or recipients_added
            or recipients_removed
        ):
            logger.info(f"Share '{share_name}' objects and recipients already match config; no update needed")
            # Update description in Databricks if changed
            current_desc = (
                (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else ""
            )
            if desc.strip() and desc.strip() != current_desc:
                desc_result = update_share_description(
                    dltshr_workspace_url=workspace_url,
                    share_name=share_name,
                    description=desc.strip(),
                )
                if desc_result is not None:
                    logger.warning(f"Share description update for '{share_name}': {desc_result}")
            db_entries.append(
                {
                    "action": "matching",
                    "share_name": share_name,
                    # Use existing.name (Databricks-returned value) to match what was stored on creation.
                    # share_name from YAML may differ in case (e.g. "WD_Share" vs "wd_share"),
                    # causing false SCD2 versions on every run.
                    "databricks_share_id": existing.name
                    if hasattr(existing, "name") and existing.name
                    else share_name,
                    "description": desc,
                    "storage_root": "",
                    "share_assets": desired_share_assets,
                    "recipients_attached": desired_recipients,
                    "ext_catalog_name": ext_catalog_name,
                    "ext_schema_name": ext_schema_name,
                    "prefix_assetname": prefix_assetname,
                    "share_tags": share_tags,
                }
            )
            created_resources["shares"].append(f"{share_name} (already matching)")
            continue

        rollback_list.append(
            (
                "updated",
                share_name,
                objects_added,
                objects_removed,
                recipients_added,
                recipients_removed,
            )
        )

        # Update description in Databricks if changed
        current_desc = (existing.comment or "").strip() if hasattr(existing, "comment") and existing.comment else ""
        if desc.strip() and desc.strip() != current_desc:
            desc_result = update_share_description(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                description=desc.strip(),
            )
            if desc_result is not None:
                logger.warning(f"Share description update for '{share_name}': {desc_result}")

        if objects_added.get("tables") or objects_added.get("views") or objects_added.get("schemas"):
            add_result = add_data_object_to_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                objects_to_add=objects_added,
            )
            if isinstance(add_result, str) and "already" not in add_result.lower():
                raise RuntimeError(f"Failed to add data objects to share {share_name}: {add_result}")
        for rec in recipients_added:
            add_rec_result = add_recipients_to_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                recipient_name=rec,
            )
            if isinstance(add_rec_result, str) and "already" not in add_rec_result.lower():
                raise RuntimeError(f"Failed to add recipient {rec} to share {share_name}: {add_rec_result}")
        if objects_removed.get("tables") or objects_removed.get("views") or objects_removed.get("schemas"):
            revoke_result = revoke_data_object_from_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                objects_to_revoke=objects_removed,
            )
            if isinstance(revoke_result, str):
                raise RuntimeError(f"Failed to revoke data objects from share {share_name}: {revoke_result}")
        for rec in recipients_removed:
            rem_result = remove_recipients_from_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                recipient_name=rec,
            )
            if isinstance(rem_result, str) and "not found" not in rem_result.lower():
                raise RuntimeError(f"Failed to remove recipient {rec} from share {share_name}: {rem_result}")

        # Collect removed assets so the orchestrator can delete their pipelines (DB-first,
        # Databricks fallback) after all share Databricks ops succeed.
        all_removed_assets = to_remove_tables + to_remove_views + to_remove_schemas
        if all_removed_assets and removed_assets_per_share is not None:
            removed_assets_per_share.append(
                {
                    "share_name": share_name,
                    "removed_assets": all_removed_assets,
                    "ext_catalog_name": ext_catalog_name,
                    "ext_schema_name": ext_schema_name,
                }
            )

        # Collect newly added assets so the orchestrator can verify/sync their pipelines.
        all_added_assets = to_add_tables + to_add_views + to_add_schemas
        if all_added_assets and added_assets_per_share is not None:
            added_assets_per_share.append(
                {
                    "share_name": share_name,
                    "added_assets": all_added_assets,
                    "ext_catalog_name": ext_catalog_name,
                    "ext_schema_name": ext_schema_name,
                }
            )

        created_resources["shares"].append(f"{share_name} (updated)")
        logger.success(f"Updated share: {share_name}")

        # Re-read actual objects from Databricks after modifications for accurate DB state
        actual_objects = get_share_objects(share_name=share_name, dltshr_workspace_url=workspace_url)
        actual_assets = (
            actual_objects.get("tables", []) + actual_objects.get("views", []) + actual_objects.get("schemas", [])
        )
        actual_recipients = get_share_recipients(share_name=share_name, dltshr_workspace_url=workspace_url)

        db_entries.append(
            {
                "action": "updated",
                "share_name": share_name,
                # Use existing.name (Databricks-returned value) to match what was stored on creation.
                # share_name from YAML may differ in case, causing false SCD2 versions.
                "databricks_share_id": existing.name if hasattr(existing, "name") and existing.name else share_name,
                "description": desc,
                "storage_root": "",
                "share_assets": actual_assets,
                "recipients_attached": actual_recipients,
                "ext_catalog_name": ext_catalog_name,
                "ext_schema_name": ext_schema_name,
                "prefix_assetname": prefix_assetname,
                "share_tags": share_tags,
            }
        )
