"""
Share Pack Provisioning - UPDATE Strategy.

Implements selective update functionality:
- Update only what's specified in YAML
- Leave other resources untouched
- Idempotent and safe
- Tracks all resources in database
- Rolls back on failure
"""

import asyncio
from typing import Any
from typing import Dict
from typing import List
from uuid import UUID
from uuid import uuid4

from loguru import logger

from dbrx_api.jobs.dbrx_pipelines import create_pipeline
from dbrx_api.jobs.dbrx_pipelines import list_pipelines_with_search_criteria
from dbrx_api.jobs.dbrx_pipelines import update_pipeline_target_configuration
from dbrx_api.jobs.dbrx_schedule import create_schedule_for_pipeline
from dbrx_api.jobs.dbrx_schedule import list_schedules
from dbrx_api.jobs.dbrx_schedule import update_schedule_for_pipeline
from dbrx_api.jobs.dbrx_schedule import update_timezone_for_schedule
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository
from dbrx_api.workflow.db.repository_recipient import RecipientRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from dbrx_api.workflow.orchestrator.db_persist import persist_pipelines_to_db
from dbrx_api.workflow.orchestrator.db_persist import persist_recipients_to_db
from dbrx_api.workflow.orchestrator.db_persist import persist_shares_to_db
from dbrx_api.workflow.orchestrator.db_persist import propagate_share_ids_to_pipelines
from dbrx_api.workflow.orchestrator.pipeline_cleanup import cleanup_orphaned_pipelines
from dbrx_api.workflow.orchestrator.pipeline_flow import _rollback_pipelines
from dbrx_api.workflow.orchestrator.pipeline_flow import check_and_sync_pipelines_for_added_assets
from dbrx_api.workflow.orchestrator.pipeline_flow import delete_pipelines_for_removed_assets
from dbrx_api.workflow.orchestrator.pipeline_flow import ensure_pipelines
from dbrx_api.workflow.orchestrator.provisioning import validate_metadata
from dbrx_api.workflow.orchestrator.provisioning import validate_sharepack_config
from dbrx_api.workflow.orchestrator.recipient_flow import _rollback_recipients
from dbrx_api.workflow.orchestrator.recipient_flow import ensure_recipients
from dbrx_api.workflow.orchestrator.share_flow import _rollback_shares
from dbrx_api.workflow.orchestrator.share_flow import ensure_shares
from dbrx_api.workflow.orchestrator.status_tracker import StatusTracker


async def provision_sharepack_update(pool, share_pack: Dict[str, Any]):
    """
    Provision a share pack using UPDATE strategy (selective updates).

    Only updates resources that are present in the YAML config.
    Sections not present in YAML are left unchanged.

    Supports updating:
    - Recipients (IP lists, descriptions)
    - Shares (data objects, recipient permissions)
    - Pipelines (configuration, tags)
    - Schedules (cron expression, timezone)

    Args:
        pool: asyncpg connection pool
        share_pack: Share pack dict from database (includes config as JSONB)

    Raises:
        Exception: If update fails
    """
    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)

    # Initialize repositories
    recipient_repo = RecipientRepository(pool)
    share_repo = ShareRepository(pool)
    pipeline_repo = PipelineRepository(pool)

    # Track updated resources
    updated_resources = {
        "recipients": [],
        "shares": [],
        "data_objects": [],
        "permissions": [],
        "pipelines": [],
        "schedules": [],
    }

    current_step = ""
    recipient_rollback_list = []
    recipient_db_entries = []
    share_rollback_list = []
    share_db_entries = []
    pipeline_rollback_list = []
    pipeline_db_entries = []
    removed_assets_per_share: list = []
    added_assets_per_share: list = []

    try:
        import json

        # Parse config if it's a JSON string
        config = share_pack["config"]
        if isinstance(config, str):
            config = json.loads(config)

        workspace_url = config["metadata"]["workspace_url"]

        logger.info(f"Starting UPDATE strategy provisioning for {share_pack_id}")
        logger.info(f"Target workspace: {workspace_url}")

        # Validate metadata before proceeding
        current_step = "Step 0/8: Validating metadata and configuration"
        await tracker.update(current_step)
        validate_metadata(config["metadata"])
        validate_sharepack_config(config)

        # Detect which sections are present
        has_recipients = "recipient" in config and config["recipient"]
        has_shares = "share" in config and config["share"]

        logger.info(f"Update scope: recipients={has_recipients}, shares={has_shares}")

        # Step 1: Initialize
        current_step = "Step 1/8: Initializing update"
        await tracker.update(current_step)

        # Step 2: Ensure recipients (Databricks only — no DB writes)
        if has_recipients:
            current_step = "Step 2/8: Creating/updating recipients"
            await tracker.update(current_step)
            await ensure_recipients(
                workspace_url=workspace_url,
                recipients_config=config["recipient"],
                rollback_list=recipient_rollback_list,
                db_entries=recipient_db_entries,
                created_resources=updated_resources,
            )
        else:
            logger.info("No recipients section - skipping recipient updates")
            await tracker.update("Step 2/8: Skipping recipients (not in config)")

        # Step 3: Ensure shares (Databricks only — no DB writes)
        if has_shares:
            current_step = "Step 3/8: Creating/updating shares"
            await tracker.update(current_step)
            await ensure_shares(
                workspace_url=workspace_url,
                shares_config=config["share"],
                rollback_list=share_rollback_list,
                db_entries=share_db_entries,
                created_resources=updated_resources,
                removed_assets_per_share=removed_assets_per_share,
                added_assets_per_share=added_assets_per_share,
            )

            # Step 3.5: Delete pipelines for removed assets (DB-first, Databricks fallback)
            for item in removed_assets_per_share:
                deleted_pipelines = await delete_pipelines_for_removed_assets(
                    workspace_url=workspace_url,
                    share_name=item["share_name"],
                    removed_assets=item["removed_assets"],
                    pipeline_repo=pipeline_repo,
                    ext_catalog_name=item.get("ext_catalog_name"),
                    ext_schema_name=item.get("ext_schema_name"),
                )
                if deleted_pipelines:
                    updated_resources.setdefault("deleted_pipelines", []).extend(deleted_pipelines)
                    logger.info(
                        f"Deleted {len(deleted_pipelines)} pipeline(s) for removed assets "
                        f"from share '{item['share_name']}': {deleted_pipelines}"
                    )
        else:
            logger.info("No shares section - skipping share updates")
            await tracker.update("Step 3/8: Skipping shares (not in config)")

        # Step 4: Ensure pipelines (Databricks only — no DB writes)
        if has_shares:
            current_step = "Step 4/8: Updating pipelines"
            await tracker.update(current_step)
            await ensure_pipelines(
                workspace_url=workspace_url,
                shares_config=config["share"],
                rollback_list=pipeline_rollback_list,
                db_entries=pipeline_db_entries,
                created_resources=updated_resources,
            )

            # Step 4.5b: Verify every newly added share asset has a pipeline.
            # Checks YAML config (pipeline_db_entries), DB, then Databricks API.
            # If a pipeline exists in Databricks but not DB, adds a minimal DB entry.
            # If no pipeline is found for any asset, raises ValueError → triggers rollback
            # of all Databricks changes (pipelines, shares, recipients) made this run.
            if added_assets_per_share:
                current_step = "Step 4.5b/8: Verifying pipelines for newly added share assets"
                await tracker.update(current_step)
                await check_and_sync_pipelines_for_added_assets(
                    workspace_url=workspace_url,
                    added_assets_per_share=added_assets_per_share,
                    pipeline_db_entries=pipeline_db_entries,
                    pipeline_repo=pipeline_repo,
                )

            # Step 5: Report schedule updates
            await tracker.update("Step 5/8: Pipeline schedules updated")
            logger.info(f"Schedule updates: {len(updated_resources.get('schedules', []))} schedules created/updated")
        else:
            await tracker.update("Step 4/8: Skipping pipelines (not in config)")
            await tracker.update("Step 5/8: Skipping schedules (no pipelines)")

        # ALL Databricks ops succeeded → persist to DB
        current_step = "Step 6/8: Persisting to database"
        await tracker.update(current_step)

        configurator = config["metadata"]["configurator"]
        if recipient_db_entries:
            await persist_recipients_to_db(recipient_db_entries, share_pack_id, configurator, recipient_repo)
        share_name_to_id = {}
        if share_db_entries:
            share_name_to_id = await persist_shares_to_db(share_db_entries, share_pack_id, share_repo)
        if pipeline_db_entries:
            await persist_pipelines_to_db(
                pipeline_db_entries, share_pack_id, share_name_to_id, share_repo, pipeline_repo
            )
        # Propagate current share_ids to any pipeline records that pre-date this
        # provisioning run and still reference a stale (old) share_id.
        if share_name_to_id:
            await propagate_share_ids_to_pipelines(share_name_to_id, pipeline_repo)

        # Step 7: Clean up orphaned pipelines (whose assets were removed from shares)
        # Re-enabled with enhanced debug logging to diagnose share lookup issues
        if has_shares:
            current_step = "Step 7/8: Cleaning up orphaned pipelines"
            await tracker.update(current_step)
            logger.info("Starting pipeline cleanup with DEBUG logging enabled")
            try:
                await cleanup_orphaned_pipelines(
                    share_pack_id=share_pack_id,
                    workspace_url=workspace_url,
                    pipeline_repo=pipeline_repo,
                    share_repo=share_repo,
                )
            except Exception as cleanup_err:
                logger.opt(exception=True).warning(f"Pipeline cleanup failed (non-fatal): {cleanup_err}")
        else:
            await tracker.update("Step 7/8: Skipping pipeline cleanup")

        # Determine if any changes were made
        # Check if all db_entries have action='unchanged' (no changes)
        all_unchanged = True
        total_created = 0
        total_updated = 0

        for entry in recipient_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        for entry in share_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        for entry in pipeline_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        # Mark as completed with appropriate message
        if all_unchanged:
            completion_message = "Already up to date with share pack data"
            logger.info(f"Share pack {share_pack_id} is already up to date - no changes needed")
        else:
            completion_message = f"All steps completed successfully ({total_created} created, {total_updated} updated)"
            logger.success(f"Share pack {share_pack_id} updated successfully")
            logger.info(
                f"Updated: {len(updated_resources['recipients'])} recipients, "
                f"{len(updated_resources['shares'])} shares, "
                f"{len(updated_resources['pipelines'])} pipelines, "
                f"{len(updated_resources['schedules'])} schedules"
            )

        await tracker.complete(completion_message)

    except Exception as e:
        await tracker.fail(str(e), current_step or "Provisioning failed")
        logger.error(f"Update failed for {share_pack_id}: {e}", exc_info=True)
        logger.warning(f"Resources updated before failure: {updated_resources}")

        # Rollback Databricks only — no DB cleanup needed (DB was never written).
        # NOTE: Rollback involves synchronous Databricks API calls (pipeline/share/recipient
        # operations). Each call may take several seconds. With many resources, rollback
        # can take a minute or more — this is normal. Watch logs for rollback progress.
        has_rollback = bool(pipeline_rollback_list or share_rollback_list or recipient_rollback_list)
        if has_rollback:
            logger.info(
                f"Starting rollback: {len(pipeline_rollback_list)} pipeline(s), "
                f"{len(share_rollback_list)} share(s), "
                f"{len(recipient_rollback_list)} recipient(s). "
                "This may take a minute — Databricks API calls are in progress..."
            )

        if pipeline_rollback_list:
            logger.info("Rolling back pipeline changes in Databricks...")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(_rollback_pipelines, pipeline_rollback_list, workspace_url),
                    timeout=120,
                )
                logger.info("Pipeline rollback complete.")
            except asyncio.TimeoutError:
                logger.error(
                    "Pipeline rollback timed out after 120s — some pipeline changes may not have been reverted."
                )
            except Exception as rb_err:
                logger.error(f"Pipeline rollback failed: {rb_err}", exc_info=True)

        if share_rollback_list:
            logger.info("Rolling back share changes in Databricks...")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(_rollback_shares, share_rollback_list, workspace_url),
                    timeout=120,
                )
                logger.info("Share rollback complete.")
            except asyncio.TimeoutError:
                logger.error("Share rollback timed out after 120s — some share changes may not have been reverted.")
            except Exception as rb_err:
                logger.error(f"Share rollback failed: {rb_err}", exc_info=True)

        if recipient_rollback_list:
            logger.info("Rolling back recipient changes in Databricks...")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(_rollback_recipients, recipient_rollback_list, workspace_url),
                    timeout=120,
                )
                logger.info("Recipient rollback complete.")
            except asyncio.TimeoutError:
                logger.error(
                    "Recipient rollback timed out after 120s — some recipient changes may not have been reverted."
                )
            except Exception as rb_err:
                logger.error(f"Recipient rollback failed: {rb_err}", exc_info=True)

        if has_rollback:
            logger.info("All rollback operations finished.")

        raise


async def _update_pipelines_and_schedules(
    workspace_url: str,
    shares: List[Dict],
    updated_resources: Dict,
    pipeline_repo: PipelineRepository,
    share_repo: ShareRepository,
    share_pack_id: UUID,
    created_db_records: Dict,
):
    """Update pipeline configurations and schedules with database tracking."""
    for share_config in shares:
        share_name = share_config["name"]

        if "pipelines" not in share_config:
            logger.debug(f"No pipelines specified for {share_name}, skipping")
            continue

        pipelines = share_config.get("pipelines", [])
        logger.info(f"Checking {len(pipelines)} pipelines for share {share_name}")

        for pipeline_config in pipelines:
            pipeline_name = pipeline_config["name_prefix"]

            try:
                # Check if pipeline exists
                pipelines_list = list_pipelines_with_search_criteria(
                    dltshr_workspace_url=workspace_url,
                    filter_expr=pipeline_name,
                )

                pipeline_id = None
                for pipeline in pipelines_list:
                    if pipeline.name == pipeline_name:
                        pipeline_id = pipeline.pipeline_id
                        break

                if not pipeline_id:
                    # UPDATE strategy should NOT create new pipelines - only update existing ones
                    error_msg = (
                        f"Pipeline '{pipeline_name}' does not exist in Databricks. "
                        f"UPDATE strategy can only modify existing pipelines. "
                        f"Use NEW strategy to create pipelines, or remove this pipeline from the YAML."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                logger.info(f"Found existing pipeline: {pipeline_name} (id: {pipeline_id})")

                # Update pipeline configuration if needed
                await _update_pipeline_configuration(
                    workspace_url,
                    pipeline_name,
                    pipeline_id,
                    pipeline_config,
                    share_config,  # Pass the entire share config for delta_share access
                    updated_resources,
                )

                # Update schedule if present
                if "schedule" in pipeline_config:
                    logger.info(f"Processing schedule for pipeline {pipeline_name}")
                    await _update_pipeline_schedule(
                        workspace_url,
                        pipeline_name,
                        pipeline_id,
                        pipeline_config,
                        updated_resources,
                    )
                else:
                    logger.warning(f"No schedule found in config for pipeline {pipeline_name}")

            except Exception as e:
                logger.error(f"Failed to update pipeline {pipeline_name}: {e}")
                raise  # Re-raise exception to fail the entire provisioning process


async def _create_pipeline(
    workspace_url: str,
    pipeline_name: str,
    pipeline_config: Dict,
    share_config: Dict,
    updated_resources: Dict,
    pipeline_repo: PipelineRepository,
    share_repo: ShareRepository,
    share_pack_id: UUID,
    created_db_records: Dict,
) -> str | None:
    """Create a new pipeline and track in database."""
    try:
        # Extract source and target assets
        source_asset = pipeline_config.get("source_asset")
        target_asset = pipeline_config.get("target_asset")

        if not source_asset:
            error_msg = f"Missing source_asset for pipeline {pipeline_name}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Default target_asset to source table name if not specified
        if not target_asset:
            target_asset = source_asset.split(".")[-1]

        # Get catalog and schema (pipeline-level overrides or share-level defaults)
        delta_share = share_config.get("delta_share", {})
        ext_catalog = pipeline_config.get("ext_catalog_name") or delta_share.get("ext_catalog_name")
        ext_schema = pipeline_config.get("ext_schema_name") or delta_share.get("ext_schema_name")

        if not ext_catalog or not ext_schema:
            error_msg = f"Missing ext_catalog_name or ext_schema_name for pipeline {pipeline_name}. Provide in pipeline config or share delta_share section."
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Build configuration
        configuration = {
            "pipelines.source_table": source_asset,
            "pipelines.target_table": target_asset,
            "pipelines.keys": pipeline_config.get("key_columns", ""),
            "pipelines.scd_type": pipeline_config.get("scd_type", "2"),
        }

        if "apply_changes" in pipeline_config:
            configuration["pipelines.apply_changes"] = str(pipeline_config["apply_changes"]).lower()

        logger.info(f"Creating pipeline: {pipeline_name} (source: {source_asset}, target: {target_asset})")

        # Create pipeline
        result = create_pipeline(
            dltshr_workspace_url=workspace_url,
            pipeline_name=pipeline_name,
            target_catalog_name=ext_catalog,
            target_schema_name=ext_schema,
            configuration=configuration,
            notifications_list=pipeline_config.get("notification", []),
            tags=pipeline_config.get("tags", {}),
            serverless=pipeline_config.get("serverless", False),
        )

        if isinstance(result, str):
            error_msg = f"Failed to create pipeline {pipeline_name}: {result}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.success(f"Created pipeline: {pipeline_name}")
        updated_resources["pipelines"].append(f"{pipeline_name} (created)")

        # Track in database
        try:
            # Get share_id from database using share_name
            share_name = share_config["name"]
            share_records = await share_repo.list_by_share_pack(share_pack_id)
            share_id = None
            for share_record in share_records:
                if share_record["share_name"] == share_name:
                    share_id = share_record["share_id"]
                    break

            if not share_id:
                logger.warning(f"Could not find share_id for {share_name}, skipping pipeline database tracking")
            else:
                pipeline_id_db = uuid4()

                # Extract schedule info
                schedule = pipeline_config.get("schedule", {})
                cron_expr = ""
                timezone = "UTC"
                schedule_type = "CRON"

                if isinstance(schedule, dict):
                    cron_expr = schedule.get("cron", "")
                    timezone = schedule.get("timezone", "UTC")
                elif isinstance(schedule, str):
                    schedule_type = schedule.upper()

                try:
                    await pipeline_repo.create_from_config(
                        pipeline_id=pipeline_id_db,
                        share_id=share_id,
                        share_pack_id=share_pack_id,
                        pipeline_name=pipeline_name,
                        databricks_pipeline_id=result.pipeline_id,
                        asset_name=target_asset,
                        source_table=source_asset,
                        target_table=target_asset,
                        scd_type=pipeline_config.get("scd_type", "2"),
                        key_columns=pipeline_config.get("key_columns", ""),
                        schedule_type=schedule_type,
                        cron_expression=cron_expr,
                        timezone=timezone,
                        serverless=pipeline_config.get("serverless", False),
                        tags=pipeline_config.get("tags", {}),
                        notification_emails=pipeline_config.get("notification", []),
                        created_by="orchestrator",
                    )
                    logger.debug(f"Tracked pipeline {pipeline_name} in database (id: {pipeline_id_db})")
                except Exception as db_error:
                    logger.warning(
                        f"Failed to track pipeline {pipeline_name} in database (UPDATE strategy - object should exist): {db_error}"
                    )
                created_db_records["pipelines"].append(pipeline_id_db)
        except Exception as db_error:
            logger.warning(
                f"Failed to track pipeline {pipeline_name} in database (UPDATE strategy - object should exist): {db_error}"
            )

        # Return Databricks pipeline_id
        return result.pipeline_id

    except Exception as e:
        logger.error(f"Failed to create pipeline {pipeline_name}: {e}")
        raise  # Re-raise exception to fail the entire provisioning process


async def _update_pipeline_configuration(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    pipeline_config: Dict,
    share_config: Dict,
    updated_resources: Dict,
):
    """
    Update pipeline configuration - ONLY mutable fields.

    Immutable fields (will raise error if changed):
    - source_asset (source table)
    - scd_type

    Mutable fields (can be updated):
    - target_asset (target table name)
    - key_columns (validated against source table schema)
    - notifications
    - serverless
    - tags
    """
    try:
        # Get existing pipeline details
        from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name
        from dbrx_api.jobs.dbrx_pipelines import validate_pipeline_keys

        existing_pipeline = get_pipeline_by_name(workspace_url, pipeline_name)

        if isinstance(existing_pipeline, str):
            error_msg = f"Failed to get existing pipeline {pipeline_name}: {existing_pipeline}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        if not existing_pipeline or not existing_pipeline.spec:
            error_msg = f"Pipeline {pipeline_name} exists but has no spec"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        # Extract existing configuration
        existing_config = dict(existing_pipeline.spec.configuration) if existing_pipeline.spec.configuration else {}
        existing_source = existing_config.get("pipelines.source_table")
        existing_scd_type = existing_config.get("pipelines.scd_type")
        existing_keys = existing_config.get("pipelines.keys")

        logger.info(
            f"Existing pipeline config: source={existing_source}, scd_type={existing_scd_type}, keys={existing_keys}"
        )

        # Extract new configuration values
        new_source = pipeline_config.get("source_asset")
        new_target = pipeline_config.get("target_asset")
        new_scd_type = pipeline_config.get("scd_type")
        new_keys = pipeline_config.get("key_columns")

        # VALIDATION 1: Prevent source_asset changes (immutable)
        if new_source and existing_source and new_source != existing_source:
            error_msg = (
                f"Cannot change source_asset for existing pipeline '{pipeline_name}'. "
                f"Existing: '{existing_source}', Requested: '{new_source}'. "
                f"Source asset is immutable - delete and recreate the pipeline if you need to change it."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # VALIDATION 2: Prevent scd_type changes (immutable)
        if new_scd_type and existing_scd_type and new_scd_type != existing_scd_type:
            error_msg = (
                f"Cannot change scd_type for existing pipeline '{pipeline_name}'. "
                f"Existing: '{existing_scd_type}', Requested: '{new_scd_type}'. "
                f"SCD type is immutable - delete and recreate the pipeline if you need to change it."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # VALIDATION 3: Validate key_columns if changed
        if new_keys and existing_keys and new_keys != existing_keys:
            logger.info(
                f"Key columns changed for {pipeline_name}: '{existing_keys}' → '{new_keys}', validating against source table"
            )

            # Use existing source (since we can't change it)
            source_table = existing_source or new_source

            if not source_table:
                error_msg = f"Cannot validate key_columns for {pipeline_name} - no source_asset found"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Get workspace client for validation
            from datetime import datetime
            from datetime import timezone

            from databricks.sdk import WorkspaceClient

            from dbrx_api.dbrx_auth.token_gen import get_auth_token

            session_token = get_auth_token(datetime.now(timezone.utc))[0]
            w_client = WorkspaceClient(host=workspace_url, token=session_token)

            # Validate keys against source table schema
            keys_validation = validate_pipeline_keys(
                w_client=w_client,
                source_table=source_table,
                keys=new_keys,
            )

            if not keys_validation["success"]:
                error_msg = (
                    f"Invalid key_columns for pipeline '{pipeline_name}': {keys_validation['message']}. "
                    f"Invalid keys: {keys_validation['invalid_keys']}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info(f"Key columns validation passed for {pipeline_name}: {keys_validation['valid_keys']}")

        # Build updated configuration (only mutable fields)
        configuration = existing_config.copy()  # Start with existing config

        # Update target_table if provided
        if new_target:
            configuration["pipelines.target_table"] = new_target
            logger.info(f"Updating target_table: {new_target}")

        # Update key_columns if provided and validated
        if new_keys:
            configuration["pipelines.keys"] = new_keys
            logger.info(f"Updating key_columns: {new_keys}")

        # Get catalog and schema (pipeline-level overrides or share-level defaults)
        delta_share = share_config.get("delta_share", {})
        ext_catalog = (
            pipeline_config.get("ext_catalog_name")
            or delta_share.get("ext_catalog_name")
            or existing_pipeline.spec.catalog
        )
        ext_schema = (
            pipeline_config.get("ext_schema_name")
            or delta_share.get("ext_schema_name")
            or existing_pipeline.spec.target
        )

        logger.info(f"Updating pipeline configuration for {pipeline_name}")
        logger.debug(f"Updated configuration: {configuration}")

        # Extract libraries from existing pipeline (required by Databricks)
        libraries = existing_pipeline.spec.libraries if existing_pipeline.spec else None
        if not libraries:
            logger.warning(f"No libraries found in existing pipeline {pipeline_name}")

        # Build notifications list
        notifications_list = pipeline_config.get("notification", [])
        notifications = None
        if notifications_list:
            from databricks.sdk.service.pipelines import Notifications

            notifications = [
                Notifications(
                    email_recipients=notifications_list,
                    alerts=[
                        "on-update-failure",
                        "on-update-fatal-failure",
                        "on-update-success",
                        "on-flow-failure",
                    ],
                )
            ]

        # Update pipeline configuration
        result = update_pipeline_target_configuration(
            dltshr_workspace_url=workspace_url,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            configuration=configuration,
            catalog=ext_catalog,
            target=ext_schema,
            libraries=libraries,
            notifications=notifications,
            tags=pipeline_config.get("tags"),
            serverless=pipeline_config.get("serverless"),
        )

        if isinstance(result, str):
            error_msg = f"Failed to update pipeline config for {pipeline_name}: {result}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        else:
            logger.success(f"Updated configuration for pipeline: {pipeline_name}")
            updated_resources["pipelines"].append(f"{pipeline_name} (config)")

    except Exception as e:
        logger.error(f"Failed to update pipeline configuration for {pipeline_name}: {e}")
        raise  # Re-raise exception to fail the entire provisioning process


async def _update_pipeline_schedule(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    pipeline_config: Dict,
    updated_resources: Dict,
):
    """
    Update, add, or remove schedule for a pipeline.

    Supports three operations:
    1. **Remove**: schedule = {"action": "remove"} - Deletes all schedules for the pipeline
    2. **Add**: schedule = {"cron": "...", "timezone": "..."} - Creates new schedule (if none exists)
    3. **Update**: schedule = {"cron": "...", "timezone": "..."} - Updates existing schedule (if exists)

    Examples:
        # Remove schedule
        schedule:
          action: "remove"

        # Add or update schedule
        schedule:
          cron: "0 0 0 * * ?"
          timezone: "UTC"
    """
    schedule = pipeline_config.get("schedule")

    if not schedule:
        logger.debug(f"No schedule config for {pipeline_name}, skipping schedule management")
        return

    try:
        # Get existing schedules for this pipeline
        schedules, _ = list_schedules(
            dltshr_workspace_url=workspace_url,
            pipeline_id=pipeline_id,
        )

        # Check if action is "remove"
        if isinstance(schedule, dict) and schedule.get("action") == "remove":
            if not schedules:
                logger.info(f"No schedules to remove for {pipeline_name}")
                return

            # Remove all schedules for this pipeline
            logger.info(f"Removing all schedules for {pipeline_name} ({len(schedules)} schedule(s))")
            from dbrx_api.jobs.dbrx_schedule import delete_schedule_for_pipeline

            result = delete_schedule_for_pipeline(
                dltshr_workspace_url=workspace_url,
                pipeline_id=pipeline_id,
            )

            # Check for success messages (deleted, successfully) or "no schedules found" (benign)
            if (
                "deleted" in result.lower()
                or "successfully" in result.lower()
                or "no schedules found" in result.lower()
            ):
                logger.success(f"Removed schedules for {pipeline_name}: {result}")
                updated_resources["schedules"].append(f"{pipeline_name} (removed)")
            elif "error" in result.lower():
                # Only raise if it's an actual error, not "no schedules found"
                error_msg = f"Failed to remove schedules for {pipeline_name}: {result}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            else:
                # Unknown result - log and continue (don't fail)
                logger.warning(f"Schedule removal result for {pipeline_name}: {result}")
                updated_resources["schedules"].append(f"{pipeline_name} (removal attempted)")
            return

        # Extract cron and timezone from schedule config
        if isinstance(schedule, dict):
            # Handle both v1.0 and v2.0 schedule formats
            new_cron = schedule.get("cron")
            new_timezone = schedule.get("timezone", "UTC")

            # v1.0 format: schedule has source_asset as key with nested cron/timezone
            if not new_cron:
                schedule_keys = [k for k in schedule.keys() if k not in ["cron", "timezone", "action"]]
                if len(schedule_keys) == 1:
                    source_asset_key = schedule_keys[0]
                    nested_schedule = schedule[source_asset_key]
                    if isinstance(nested_schedule, dict):
                        new_cron = nested_schedule.get("cron")
                        new_timezone = nested_schedule.get("timezone", "UTC")
                        logger.info(f"[v1.0 FORMAT] Extracted cron from nested schedule for {pipeline_name}")
                    elif isinstance(nested_schedule, str) and nested_schedule.lower() == "continuous":
                        logger.warning(f"[v1.0 FORMAT] Continuous schedule not yet supported for {pipeline_name}")
                        return

            if not new_cron:
                logger.warning(f"No cron expression found in schedule config for {pipeline_name}, skipping")
                return

            # Case 1: No existing schedules - CREATE new schedule
            if not schedules:
                logger.info(f"No schedule found for {pipeline_name}, creating new one")
                await _create_schedule(workspace_url, pipeline_name, pipeline_id, schedule, pipeline_config)
                updated_resources["schedules"].append(f"{pipeline_name} (created)")
                return

            # Case 2: Existing schedule(s) - UPDATE existing schedule
            job_id = schedules[0]["job_id"]
            job_name = schedules[0]["job_name"]
            existing_cron = schedules[0].get("cron_schedule", {})

            logger.info(f"Found existing schedule for {pipeline_name} (job: {job_name}), checking for updates")

            # Check if cron expression changed
            cron_changed = existing_cron.get("cron_expression") != new_cron
            timezone_changed = existing_cron.get("timezone") != new_timezone

            if cron_changed:
                logger.info(
                    f"Updating cron for {pipeline_name}: '{existing_cron.get('cron_expression')}' → '{new_cron}'"
                )
                result = update_schedule_for_pipeline(
                    dltshr_workspace_url=workspace_url,
                    job_id=job_id,
                    cron_expression=new_cron,
                )
                if isinstance(result, str) and "success" in result.lower():
                    logger.success(f"Updated cron for {pipeline_name}: {new_cron}")
                    updated_resources["schedules"].append(f"{pipeline_name} (cron updated)")
                elif isinstance(result, str):
                    error_msg = f"Failed to update cron for {pipeline_name}: {result}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                else:
                    logger.success(f"Updated cron for {pipeline_name}: {new_cron}")
                    updated_resources["schedules"].append(f"{pipeline_name} (cron updated)")

            if timezone_changed:
                logger.info(
                    f"Updating timezone for {pipeline_name}: '{existing_cron.get('timezone')}' → '{new_timezone}'"
                )
                result = update_timezone_for_schedule(
                    dltshr_workspace_url=workspace_url,
                    job_id=job_id,
                    time_zone=new_timezone,
                )
                if isinstance(result, str) and "success" in result.lower():
                    logger.success(f"Updated timezone for {pipeline_name}: {new_timezone}")
                    updated_resources["schedules"].append(f"{pipeline_name} (timezone updated)")
                elif isinstance(result, str):
                    error_msg = f"Failed to update timezone for {pipeline_name}: {result}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                else:
                    logger.success(f"Updated timezone for {pipeline_name}: {new_timezone}")
                    updated_resources["schedules"].append(f"{pipeline_name} (timezone updated)")

            if not cron_changed and not timezone_changed:
                logger.info(f"Schedule for {pipeline_name} unchanged (cron: {new_cron}, timezone: {new_timezone})")

        elif isinstance(schedule, str) and schedule.lower() == "continuous":
            logger.warning(f"Continuous schedules not yet supported for {pipeline_name}")

    except Exception as e:
        logger.error(f"Failed to update schedule for {pipeline_name}: {e}")
        raise  # Re-raise exception to fail the entire provisioning process


async def _create_schedule(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    schedule: Any,
    pipeline_config: Dict,
):
    """Create a new schedule for a pipeline."""
    logger.info(
        f"Attempting to create schedule for {pipeline_name}, schedule type: {type(schedule)}, value: {schedule}"
    )

    if isinstance(schedule, str) and schedule.lower() == "continuous":
        logger.warning(f"Continuous schedules not yet supported for {pipeline_name}")
        return

    if isinstance(schedule, dict):
        # Handle both v1.0 and v2.0 schedule formats
        cron_expression = schedule.get("cron")
        timezone = schedule.get("timezone", "UTC")

        # v1.0 format: schedule has source_asset as key with nested cron/timezone
        if not cron_expression:
            # Check if this is v1.0 format with source_asset as key
            schedule_keys = [k for k in schedule.keys() if k not in ["cron", "timezone"]]
            if len(schedule_keys) == 1:
                source_asset_key = schedule_keys[0]
                nested_schedule = schedule[source_asset_key]
                if isinstance(nested_schedule, dict):
                    cron_expression = nested_schedule.get("cron")
                    timezone = nested_schedule.get("timezone", "UTC")
                    logger.info(f"[v1.0 FORMAT] Extracted cron from nested schedule for {pipeline_name}")
                elif isinstance(nested_schedule, str):
                    # v1.0 continuous format
                    if nested_schedule.lower() == "continuous":
                        logger.warning(f"[v1.0 FORMAT] Continuous schedule not yet supported for {pipeline_name}")
                        cron_expression = None

        logger.debug(f"Extracted schedule for {pipeline_name} - cron: {cron_expression}, timezone: {timezone}")

        if cron_expression:
            job_name = f"{pipeline_name}_schedule"
            result = create_schedule_for_pipeline(
                dltshr_workspace_url=workspace_url,
                job_name=job_name,
                pipeline_id=pipeline_id,
                cron_expression=cron_expression,
                time_zone=timezone,
                paused=False,
                email_notifications=pipeline_config.get("notification", []),
                tags=pipeline_config.get("tags", {}),
                description=pipeline_config.get("description"),
            )

            if isinstance(result, str):
                if "already exists" in result.lower():
                    logger.info(f"Job {job_name} already exists - verifying schedule is active")
                    # Verify the existing schedule
                    try:
                        schedules_verify, _ = list_schedules(workspace_url, pipeline_id)
                        if schedules_verify:
                            logger.success(f"Schedule for {pipeline_name} exists and is active (job: {job_name})")
                        else:
                            logger.warning(
                                f"Job {job_name} exists but no active schedule found for pipeline - may need manual cleanup in Databricks Workflows"
                            )
                    except Exception as verify_error:
                        logger.warning(f"Could not verify schedule for {pipeline_name}: {verify_error}")
                elif "success" in result.lower() or "created" in result.lower():
                    logger.success(f"Created schedule for {pipeline_name} (job: {job_name}, cron: {cron_expression})")
                else:
                    error_msg = f"Failed to create schedule for {pipeline_name}: {result}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
            else:
                # Dict response - success
                logger.success(f"Created schedule for {pipeline_name} (job: {job_name})")
