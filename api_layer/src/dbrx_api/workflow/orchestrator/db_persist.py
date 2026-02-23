"""
Database persistence functions for the share pack orchestrator.

Called AFTER all Databricks operations succeed, ensuring DB writes only
happen on the happy path. If any ensure_* step fails, Databricks is
rolled back and no DB writes are attempted.
"""

import json
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
        recipient_id = entry.get("recipient_id")

        # If recipient_id not in entry, look it up by name
        if not recipient_id:
            try:
                existing = await recipient_repo.list_by_recipient_name(recipient_name)
                if existing:
                    recipient_id = existing[0]["recipient_id"]
            except Exception:
                pass

        try:
            # For "matching" action, Databricks is already in the correct state and the
            # DB record reflects the last successful provisioning. Skip the upsert to
            # avoid spurious SCD2 versions caused by metadata-only diffs (e.g.
            # share_pack_id gets a new UUID on every API call).
            if action == "matching":
                entry["action"] = "unchanged"
                logger.info(f"Persisted recipient '{recipient_name}' to DB (unchanged)")
                continue

            # Get current version before operation (to detect if versioning occurred)
            current_before = None
            current_record_id_before = None
            if recipient_id:
                current_before = await recipient_repo.get_current(recipient_id, include_deleted=False)
                current_record_id_before = current_before.get("record_id") if current_before else None

            if action == "created":
                await recipient_repo.create_from_config(
                    recipient_id=recipient_id,
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

            # Check if versioning actually occurred
            # If we still don't have recipient_id, look it up again after upsert
            if not recipient_id:
                try:
                    existing = await recipient_repo.list_by_recipient_name(recipient_name)
                    if existing:
                        recipient_id = existing[0]["recipient_id"]
                except Exception:
                    pass

            if recipient_id:
                current_after = await recipient_repo.get_current(recipient_id, include_deleted=False)
                current_record_id_after = current_after.get("record_id") if current_after else None

                # Update action based on what actually happened
                if current_record_id_before is None and current_record_id_after:
                    entry["action"] = "created"
                elif current_record_id_before == current_record_id_after:
                    entry["action"] = "unchanged"
                else:
                    entry["action"] = "updated"

            logger.info(f"Persisted recipient '{recipient_name}' to DB " f"({entry.get('action', 'unknown')})")
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
        share_id = entry.get("share_id")

        # If share_id not in entry, look it up by name
        if not share_id:
            try:
                existing = await share_repo.list_by_share_name(share_name)
                if existing:
                    share_id = existing[0]["share_id"]
            except Exception:
                pass

        try:
            # For "matching" action, Databricks is already in the correct state and the
            # DB record reflects the last successful provisioning. Skip the upsert to
            # avoid spurious SCD2 versions caused by metadata-only diffs (e.g.
            # share_pack_id gets a new UUID on every API call).
            if action == "matching":
                share_name_to_id[share_name] = share_id
                entry["action"] = "unchanged"
                logger.info(f"Persisted share '{share_name}' to DB (unchanged)")
                continue

            # Get current version before operation (to detect if versioning occurred)
            current_before = None
            current_record_id_before = None
            if share_id:
                current_before = await share_repo.get_current(share_id, include_deleted=False)
                current_record_id_before = current_before.get("record_id") if current_before else None

            # For updates: preserve optional metadata from the current DB record when not
            # explicitly provided in the YAML. This prevents:
            #   1. False SCD2 versions from databricks_share_id case differences
            #      (Databricks may normalise the name; old DB rows may have a different case)
            #   2. False SCD2 versions from empty-string overwrites when ext_catalog_name,
            #      ext_schema_name, prefix_assetname, or share_tags are absent from the YAML.
            if action != "created" and current_before:
                # --- databricks_share_id case fix ---
                stored_dbrx_id = (current_before.get("databricks_share_id") or "").strip()
                new_dbrx_id = (entry.get("databricks_share_id") or "").strip()
                if stored_dbrx_id and new_dbrx_id and stored_dbrx_id.lower() == new_dbrx_id.lower():
                    # Same name, only case differs — keep the value already in the DB
                    # so no SCD2 version is triggered for a cosmetic difference.
                    if stored_dbrx_id != new_dbrx_id:
                        logger.debug(
                            f"Share '{share_name}': preserving databricks_share_id case from DB "
                            f"('{new_dbrx_id}' → '{stored_dbrx_id}')"
                        )
                    entry["databricks_share_id"] = stored_dbrx_id

                # --- preserve optional metadata fields when not provided in YAML ---
                for _field in ("ext_catalog_name", "ext_schema_name", "prefix_assetname"):
                    if not entry.get(_field) and current_before.get(_field):
                        entry[_field] = current_before[_field]

                # share_tags: entry holds a list ([] when absent from YAML); DB holds a JSON string.
                if not entry.get("share_tags"):
                    _raw_tags = current_before.get("share_tags")
                    if _raw_tags:
                        if isinstance(_raw_tags, str):
                            try:
                                _parsed = json.loads(_raw_tags)
                                if _parsed:
                                    entry["share_tags"] = _parsed
                            except (json.JSONDecodeError, TypeError, ValueError):
                                pass
                        elif isinstance(_raw_tags, list) and _raw_tags:
                            entry["share_tags"] = _raw_tags

            if action == "created":
                returned_id = await share_repo.create_from_config(
                    share_id=share_id,
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

            # CRITICAL: If we don't have share_id yet, look it up NOW before setting mapping
            # This ensures we ALWAYS use the permanent share_id, never the record_id
            if not share_id:
                try:
                    existing = await share_repo.list_by_share_name(share_name)
                    if existing:
                        share_id = existing[0]["share_id"]
                        logger.debug(f"Looked up permanent share_id for '{share_name}': {share_id}")
                except Exception:
                    pass

            # NOW set the mapping with the permanent share_id (or returned_id as last resort)
            # CRITICAL: Use permanent share_id for foreign key references, NOT record_id
            final_id = share_id if share_id else returned_id
            share_name_to_id[share_name] = final_id
            if share_id != returned_id and returned_id:
                logger.info(
                    f"Share '{share_name}': Using permanent share_id={share_id} "
                    f"(not record_id={returned_id}) for pipeline references"
                )

            if share_id:
                current_after = await share_repo.get_current(share_id, include_deleted=False)
                current_record_id_after = current_after.get("record_id") if current_after else None

                # Update action based on what actually happened
                if current_record_id_before is None and current_record_id_after:
                    entry["action"] = "created"
                elif current_record_id_before == current_record_id_after:
                    entry["action"] = "unchanged"
                else:
                    entry["action"] = "updated"

            logger.info(f"Persisted share '{share_name}' to DB ({entry['action']})")
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
        pipeline_id = entry.get("pipeline_id")
        share_name = entry["share_name"]

        # If pipeline_id not in entry, look it up by name
        if not pipeline_id:
            try:
                existing = await pipeline_repo.list_by_pipeline_name(pipeline_name)
                if existing:
                    pipeline_id = existing[0]["pipeline_id"]
            except Exception:
                pass

        # Resolve share_id - CRITICAL: Always use the most recent current share_id
        share_id = share_name_to_id.get(share_name)
        if not share_id:
            # Fallback: Query database for current share
            # First try: shares in this share pack
            try:
                share_pack_shares = await share_repo.list_by_share_pack(share_pack_id)
                match = next((s for s in share_pack_shares if s["share_name"] == share_name), None)
                if match:
                    share_id = match["share_id"]
                else:
                    # Second try: shares across all share packs
                    all_share_records = await share_repo.list_by_share_name(share_name)
                    # Prefer share from same share_pack if multiple exist
                    for record in all_share_records:
                        if record.get("share_pack_id") == share_pack_id:
                            share_id = record["share_id"]
                            break
                    # If no match in same share_pack, use first current share
                    if not share_id and all_share_records:
                        share_id = all_share_records[0]["share_id"]
            except Exception:
                pass

        if not share_id:
            logger.warning(
                f"Pipeline DB write skipped for '{pipeline_name}': " f"no share_id found for share_name={share_name}"
            )
            continue

        # CRITICAL: Check if pipeline's share_id needs updating (stale reference)
        if pipeline_id:
            current_pipeline = await pipeline_repo.get_current(pipeline_id, include_deleted=False)
            if current_pipeline:
                old_share_id = current_pipeline.get("share_id")
                if old_share_id != share_id:
                    logger.warning(
                        f"Pipeline '{pipeline_name}' has STALE share_id: " f"{old_share_id} → {share_id}. Will update."
                    )
                    # Force action to 'updated' to ensure upsert updates share_id
                    entry["action"] = "updated"

        try:
            # For "matching" action (and not overridden to "updated" by the stale
            # share_id check above), pipeline is up-to-date in Databricks and DB.
            # Skip the upsert to avoid spurious SCD2 versions.
            if entry["action"] == "matching":
                entry["action"] = "unchanged"
                logger.info(f"Persisted pipeline '{pipeline_name}' to DB (unchanged)")
                continue

            # Get current version before operation (to detect if versioning occurred)
            current_before = None
            current_record_id_before = None
            if pipeline_id:
                current_before = await pipeline_repo.get_current(pipeline_id, include_deleted=False)
                current_record_id_before = current_before.get("record_id") if current_before else None

            if action == "created":
                await pipeline_repo.create_from_config(
                    pipeline_id=pipeline_id,
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

            # Check if versioning actually occurred
            # If we still don't have pipeline_id, look it up again after upsert
            if not pipeline_id:
                try:
                    existing = await pipeline_repo.list_by_pipeline_name(pipeline_name)
                    if existing:
                        pipeline_id = existing[0]["pipeline_id"]
                except Exception:
                    pass

            if pipeline_id:
                current_after = await pipeline_repo.get_current(pipeline_id, include_deleted=False)
                current_record_id_after = current_after.get("record_id") if current_after else None

                # Update action based on what actually happened
                if current_record_id_before is None and current_record_id_after:
                    entry["action"] = "created"
                elif current_record_id_before == current_record_id_after:
                    entry["action"] = "unchanged"
                else:
                    entry["action"] = "updated"

            logger.info(f"Persisted pipeline '{pipeline_name}' to DB ({entry['action']})")
        except Exception as db_err:
            logger.opt(exception=True).warning(
                f"Failed to persist pipeline '{pipeline_name}' ({action}) " f"to DB: {db_err}"
            )


async def propagate_share_ids_to_pipelines(
    share_name_to_id: Dict[str, UUID],
    pipeline_repo: Any,
) -> None:
    """
    Ensure all active pipeline records use the current share_id for their share.

    When a share is recreated and gets a new share_id, pipeline records created
    under the previous provisioning still reference the old share_id. These stale
    references prevent share_id-based lookups (e.g. during DELETE provisioning) from
    finding all pipelines that belong to the share.

    This function is called after persist_shares_to_db and persist_pipelines_to_db.
    At that point share_name_to_id contains the definitive current share_id for every
    share that was just provisioned. For each share we:

    1. Query all active pipelines via list_by_share_name, which internally searches
       across ALL historical share_ids for the share name, so stale records are found.
    2. For any pipeline whose share_id differs from the current share_id, rewrite the
       record via upsert_from_config, producing a new SCD2 version with the correct
       share_id and otherwise identical fields.

    Pipelines that were explicitly written by persist_pipelines_to_db in this same run
    will already have the correct share_id and are silently skipped (no duplicate write).
    """
    for share_name, current_share_id in share_name_to_id.items():
        try:
            all_pipelines = await pipeline_repo.list_by_share_name(share_name)
            stale = [p for p in all_pipelines if p.get("share_id") != current_share_id]
            if not stale:
                continue

            logger.info(
                "Propagating share_id {} to {} stale pipeline record(s) for share '{}'",
                current_share_id,
                len(stale),
                share_name,
            )
            for pipeline_rec in stale:
                pipeline_name = pipeline_rec["pipeline_name"]
                try:
                    raw_tags = pipeline_rec.get("tags") or "{}"
                    tags = json.loads(raw_tags) if isinstance(raw_tags, str) else (raw_tags or {})
                    raw_notifs = pipeline_rec.get("notification_list") or "[]"
                    notifs = json.loads(raw_notifs) if isinstance(raw_notifs, str) else (raw_notifs or [])
                    await pipeline_repo.upsert_from_config(
                        share_id=current_share_id,
                        share_pack_id=pipeline_rec.get("share_pack_id"),
                        pipeline_name=pipeline_name,
                        databricks_pipeline_id=pipeline_rec.get("databricks_pipeline_id", ""),
                        asset_name=pipeline_rec.get("asset_name", ""),
                        source_table=pipeline_rec.get("source_table", ""),
                        target_table=pipeline_rec.get("target_table", ""),
                        scd_type=str(pipeline_rec.get("scd_type") or "2"),
                        key_columns=pipeline_rec.get("key_columns", ""),
                        schedule_type=pipeline_rec.get("schedule_type", "CRON"),
                        cron_expression=pipeline_rec.get("cron_expression", ""),
                        timezone=pipeline_rec.get("cron_timezone", "UTC"),
                        serverless=bool(pipeline_rec.get("serverless", False)),
                        tags=tags,
                        notification_emails=notifs,
                        created_by="orchestrator",
                        pipeline_id=pipeline_rec["pipeline_id"],
                    )
                    logger.info(
                        "Updated share_id for pipeline '{}': {} → {}",
                        pipeline_name,
                        pipeline_rec.get("share_id"),
                        current_share_id,
                    )
                except Exception as pipe_err:  # pylint: disable=broad-except
                    logger.warning(
                        "Failed to propagate share_id to pipeline '{}': {}",
                        pipeline_name,
                        pipe_err,
                    )
        except Exception as share_err:  # pylint: disable=broad-except
            logger.warning(
                "Failed to propagate share_id for share '{}': {}",
                share_name,
                share_err,
            )
