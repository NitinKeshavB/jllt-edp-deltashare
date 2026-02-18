"""
Database persistence functions for the share pack orchestrator.

Called AFTER all Databricks operations succeed, ensuring DB writes only
happen on the happy path. If any ensure_* step fails, Databricks is
rolled back and no DB writes are attempted.
"""

from typing import Any
from typing import Dict
from typing import List
from uuid import UUID

from loguru import logger


async def persist_recipients_to_db(
    db_entries: List[Dict[str, Any]],
    share_pack_id: UUID,
    configurator: str,
    recipient_repo: Any,
) -> None:
    """Persist recipient db_entries to the database after all Databricks ops succeed."""
    for entry in db_entries:
        action = entry["action"]
        recipient_name = entry["recipient_name"]
        try:
            if action == "created":
                await recipient_repo.create_from_config(
                    recipient_id=entry["recipient_id"],
                    share_pack_id=share_pack_id,
                    recipient_name=recipient_name,
                    databricks_recipient_id=entry["databricks_recipient_id"],
                    recipient_contact_email=configurator,
                    recipient_type=entry["recipient_type"],
                    recipient_databricks_org=entry.get("recipient_databricks_org"),
                    ip_access_list=entry.get("ip_access_list", []),
                    token_expiry_days=entry.get("token_expiry_days", 0),
                    token_rotation_enabled=entry.get("token_rotation_enabled", False),
                    description=entry.get("description", ""),
                    created_by="orchestrator",
                )
            else:
                await recipient_repo.upsert_from_config(
                    share_pack_id=share_pack_id,
                    recipient_name=recipient_name,
                    databricks_recipient_id=entry["databricks_recipient_id"],
                    recipient_contact_email=configurator,
                    recipient_type=entry["recipient_type"],
                    recipient_databricks_org=entry.get("recipient_databricks_org"),
                    ip_access_list=entry.get("ip_access_list", []),
                    token_expiry_days=entry.get("token_expiry_days", 0),
                    token_rotation_enabled=entry.get("token_rotation_enabled", False),
                    description=entry.get("description", ""),
                    created_by="orchestrator",
                    recipient_id=None,
                )
            logger.info(f"Persisted recipient '{recipient_name}' to DB ({action})")
        except Exception as db_err:
            logger.opt(exception=True).warning(
                f"Failed to persist recipient '{recipient_name}' ({action}) to DB: {db_err}"
            )


async def persist_shares_to_db(
    db_entries: List[Dict[str, Any]],
    share_pack_id: UUID,
    share_repo: Any,
) -> Dict[str, UUID]:
    """
    Persist share db_entries to the database after all Databricks ops succeed.

    Returns:
        Mapping of share_name -> share_id for pipeline DB writes.
    """
    share_name_to_id: Dict[str, UUID] = {}

    for entry in db_entries:
        action = entry["action"]
        share_name = entry["share_name"]
        try:
            if action == "created":
                returned_id = await share_repo.create_from_config(
                    share_id=entry["share_id"],
                    share_pack_id=share_pack_id,
                    share_name=share_name,
                    databricks_share_id=entry["databricks_share_id"],
                    description=entry.get("description", ""),
                    storage_root=entry.get("storage_root", ""),
                    share_assets=entry.get("share_assets", []),
                    recipients_attached=entry.get("recipients_attached", []),
                    ext_catalog_name=entry.get("ext_catalog_name", ""),
                    ext_schema_name=entry.get("ext_schema_name", ""),
                    prefix_assetname=entry.get("prefix_assetname", ""),
                    share_tags=entry.get("share_tags", []),
                    created_by="orchestrator",
                )
                share_name_to_id[share_name] = returned_id or entry["share_id"]
            else:
                returned_id = await share_repo.upsert_from_config(
                    share_pack_id=share_pack_id,
                    share_name=share_name,
                    databricks_share_id=entry["databricks_share_id"],
                    share_assets=entry.get("share_assets", []),
                    recipients_attached=entry.get("recipients_attached", []),
                    description=entry.get("description", ""),
                    ext_catalog_name=entry.get("ext_catalog_name", ""),
                    ext_schema_name=entry.get("ext_schema_name", ""),
                    prefix_assetname=entry.get("prefix_assetname", ""),
                    share_tags=entry.get("share_tags", []),
                    created_by="orchestrator",
                    share_id=None,
                )
                share_name_to_id[share_name] = returned_id
            logger.info(f"Persisted share '{share_name}' to DB ({action})")
        except Exception as db_err:
            logger.opt(exception=True).warning(f"Failed to persist share '{share_name}' ({action}) to DB: {db_err}")

    return share_name_to_id


async def persist_pipelines_to_db(
    db_entries: List[Dict[str, Any]],
    share_pack_id: UUID,
    share_name_to_id: Dict[str, UUID],
    share_repo: Any,
    pipeline_repo: Any,
) -> None:
    """Persist pipeline db_entries to the database after all Databricks ops succeed."""
    for entry in db_entries:
        action = entry["action"]
        pipeline_name = entry["pipeline_name"]
        share_name = entry["share_name"]

        # Resolve share_id
        share_id = share_name_to_id.get(share_name)
        if not share_id:
            try:
                all_share_records = await share_repo.list_by_share_name(share_name)
                if all_share_records:
                    share_id = all_share_records[0]["share_id"]
            except Exception:
                pass

        if not share_id:
            logger.warning(
                f"Pipeline DB write skipped for '{pipeline_name}': " f"no share_id found for share_name={share_name}"
            )
            continue

        try:
            if action == "created":
                await pipeline_repo.create_from_config(
                    pipeline_id=entry["pipeline_id"],
                    share_id=share_id,
                    share_pack_id=share_pack_id,
                    pipeline_name=pipeline_name,
                    databricks_pipeline_id=entry["databricks_pipeline_id"],
                    asset_name=entry.get("asset_name", ""),
                    source_table=entry.get("source_table", ""),
                    target_table=entry.get("target_table", ""),
                    scd_type=entry.get("scd_type", "2"),
                    key_columns=entry.get("key_columns", ""),
                    schedule_type=entry.get("schedule_type", "CRON"),
                    cron_expression=entry.get("cron_expression", ""),
                    timezone=entry.get("timezone", "UTC"),
                    serverless=entry.get("serverless", False),
                    tags=entry.get("tags", {}),
                    notification_emails=entry.get("notification_emails", []),
                    created_by="orchestrator",
                )
            else:
                await pipeline_repo.upsert_from_config(
                    share_id=share_id,
                    share_pack_id=share_pack_id,
                    pipeline_name=pipeline_name,
                    databricks_pipeline_id=entry["databricks_pipeline_id"],
                    asset_name=entry.get("asset_name", ""),
                    source_table=entry.get("source_table", ""),
                    target_table=entry.get("target_table", ""),
                    scd_type=entry.get("scd_type", "2"),
                    key_columns=entry.get("key_columns", ""),
                    schedule_type=entry.get("schedule_type", "CRON"),
                    cron_expression=entry.get("cron_expression", ""),
                    timezone=entry.get("timezone", "UTC"),
                    serverless=entry.get("serverless", False),
                    tags=entry.get("tags"),
                    notification_emails=entry.get("notification_emails", []),
                    created_by="orchestrator",
                )
            logger.info(f"Persisted pipeline '{pipeline_name}' to DB ({action})")
        except Exception as db_err:
            logger.opt(exception=True).warning(
                f"Failed to persist pipeline '{pipeline_name}' ({action}) to DB: {db_err}"
            )
