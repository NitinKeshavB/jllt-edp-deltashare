"""
Share Pack Provisioning - UPDATE Strategy.

Implements selective update functionality:
- Update only what's specified in YAML
- Leave other resources untouched
- Idempotent and safe
- Tracks all resources in database
- Rolls back on failure
"""

from typing import Any, Dict, List
from uuid import UUID, uuid4

from loguru import logger

from dbrx_api.workflow.db.repository_recipient import RecipientRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository

from dbrx_api.dltshr.recipient import (
    get_recipients,
    update_recipient_description,
    add_recipient_ip,
    revoke_recipient_ip,
    create_recipient_d2d,
    create_recipient_d2o,
)
from dbrx_api.dltshr.share import (
    get_shares,
    add_data_object_to_share,
    add_recipients_to_share,
    create_share,
)
from dbrx_api.jobs.dbrx_pipelines import (
    list_pipelines_with_search_criteria,
    update_pipeline_target_configuration,
    create_pipeline,
)
from dbrx_api.jobs.dbrx_schedule import (
    list_schedules,
    update_schedule_for_pipeline,
    update_timezone_for_schedule,
    create_schedule_for_pipeline,
)
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

    # Track database IDs for rollback
    created_db_records = {
        "recipients": [],  # List of (recipient_id, databricks_recipient_id)
        "shares": [],  # List of (share_id, databricks_share_id)
        "pipelines": [],  # List of (pipeline_id, databricks_pipeline_id)
    }

    try:
        import json

        # Parse config if it's a JSON string
        config = share_pack["config"]
        if isinstance(config, str):
            config = json.loads(config)

        workspace_url = config["metadata"]["workspace_url"]

        logger.info(f"Starting UPDATE strategy provisioning for {share_pack_id}")
        logger.info(f"Target workspace: {workspace_url}")

        # Detect which sections are present
        has_recipients = "recipient" in config and config["recipient"]
        has_shares = "share" in config and config["share"]

        logger.info(
            f"Update scope: recipients={has_recipients}, shares={has_shares}"
        )

        # Step 1: Initialize
        await tracker.update("Step 1/7: Initializing update")

        # Step 2: Update Recipients (if present)
        if has_recipients:
            await tracker.update("Step 2/7: Updating recipients")
            await _update_recipients(
                workspace_url,
                config["recipient"],
                updated_resources,
                recipient_repo,
                share_pack_id,
                created_db_records,
            )
        else:
            logger.info("No recipients section - skipping recipient updates")
            await tracker.update("Step 2/7: Skipping recipients (not in config)")

        # Step 3: Update Shares (if present)
        if has_shares:
            await tracker.update("Step 3/7: Updating shares")
            await _update_shares(
                workspace_url,
                config["share"],
                updated_resources,
                share_repo,
                share_pack_id,
                created_db_records,
            )
        else:
            logger.info("No shares section - skipping share updates")
            await tracker.update("Step 3/7: Skipping shares (not in config)")

        # Step 4: Update Share Data Objects (if shares present)
        if has_shares:
            await tracker.update("Step 4/7: Updating share data objects")
            await _update_share_data_objects(
                workspace_url, config["share"], updated_resources
            )
        else:
            await tracker.update("Step 4/7: Skipping data objects (not in config)")

        # Step 5: Update Share Permissions (if shares present)
        if has_shares:
            await tracker.update("Step 5/7: Updating share permissions")
            await _update_share_permissions(
                workspace_url, config["share"], updated_resources
            )
        else:
            await tracker.update("Step 5/7: Skipping permissions (not in config)")

        # Step 6: Update Pipelines (if shares present)
        if has_shares:
            await tracker.update("Step 6/7: Updating pipelines")
            await _update_pipelines_and_schedules(
                workspace_url,
                config["share"],
                updated_resources,
                pipeline_repo,
                share_repo,
                share_pack_id,
                created_db_records,
            )

            # Step 7: Report schedule updates
            await tracker.update("Step 7/7: Pipeline schedules updated")
            logger.info(f"Schedule updates: {len(updated_resources.get('schedules', []))} schedules created/updated")
        else:
            await tracker.update("Step 6/7: Skipping pipelines (not in config)")
            await tracker.update("Step 7/7: Skipping schedules (no pipelines)")

        # Mark as completed
        await tracker.complete()

        logger.success(f"Share pack {share_pack_id} updated successfully")
        logger.info(
            f"Updated: {len(updated_resources['recipients'])} recipients, "
            f"{len(updated_resources['shares'])} shares, "
            f"{len(updated_resources['pipelines'])} pipelines, "
            f"{len(updated_resources['schedules'])} schedules"
        )

    except Exception as e:
        await tracker.fail(str(e))
        logger.error(f"Update failed for {share_pack_id}: {e}", exc_info=True)
        logger.warning(f"Resources updated before failure: {updated_resources}")

        # Rollback: Mark created database records as deleted
        logger.info("Starting rollback of database records")
        try:
            # Rollback recipients
            for recipient_id in created_db_records["recipients"]:
                try:
                    await recipient_repo.soft_delete(
                        recipient_id,
                        deleted_by="orchestrator",
                        deletion_reason=f"Rollback due to provisioning failure: {str(e)[:200]}",
                    )
                    logger.info(f"Rolled back recipient {recipient_id}")
                except Exception as rb_error:
                    logger.error(f"Failed to rollback recipient {recipient_id}: {rb_error}")

            # Rollback shares
            for share_id in created_db_records["shares"]:
                try:
                    await share_repo.soft_delete(
                        share_id,
                        deleted_by="orchestrator",
                        deletion_reason=f"Rollback due to provisioning failure: {str(e)[:200]}",
                    )
                    logger.info(f"Rolled back share {share_id}")
                except Exception as rb_error:
                    logger.error(f"Failed to rollback share {share_id}: {rb_error}")

            # Rollback pipelines
            for pipeline_id in created_db_records["pipelines"]:
                try:
                    await pipeline_repo.soft_delete(
                        pipeline_id,
                        deleted_by="orchestrator",
                        deletion_reason=f"Rollback due to provisioning failure: {str(e)[:200]}",
                    )
                    logger.info(f"Rolled back pipeline {pipeline_id}")
                except Exception as rb_error:
                    logger.error(f"Failed to rollback pipeline {pipeline_id}: {rb_error}")

            logger.success("Database rollback completed")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}", exc_info=True)

        raise


async def _update_recipients(
    workspace_url: str,
    recipients: List[Dict],
    updated_resources: Dict,
    recipient_repo: RecipientRepository,
    share_pack_id: UUID,
    created_db_records: Dict,
):
    """Update recipient configurations and track in database."""
    for recip_config in recipients:
        recipient_name = recip_config["name"]
        recipient_type = recip_config["type"]

        logger.info(f"Checking recipient: {recipient_name}")

        try:
            # Get existing recipient
            existing = get_recipients(recipient_name, workspace_url)

            if not existing:
                logger.info(f"Recipient {recipient_name} not found - creating new recipient")
                # Create new recipient based on type
                if recipient_type == "D2D":
                    metastore_id = recip_config.get("data_recipient_global_metastore_id")
                    if not metastore_id:
                        logger.error(f"Missing data_recipient_global_metastore_id for D2D recipient {recipient_name}")
                        continue
                    result = create_recipient_d2d(
                        dltshr_workspace_url=workspace_url,
                        recipient_name=recipient_name,
                        recipient_identifier=metastore_id,
                        description=recip_config.get("description") or recip_config.get("comment", ""),
                    )
                else:  # D2O
                    result = create_recipient_d2o(
                        dltshr_workspace_url=workspace_url,
                        recipient_name=recipient_name,
                        description=recip_config.get("description") or recip_config.get("comment", ""),
                        ip_access_list=recip_config.get("recipient_ips", []),
                    )

                if isinstance(result, str):
                    logger.error(f"Failed to create recipient {recipient_name}: {result}")
                    continue
                else:
                    logger.success(f"Created recipient: {recipient_name}")
                    updated_resources["recipients"].append(f"{recipient_name} (created)")

                    # Get the newly created recipient for further updates
                    existing = get_recipients(recipient_name, workspace_url)
                    if not existing:
                        continue

                    # Track in database
                    try:
                        recipient_id = uuid4()
                        await recipient_repo.create_from_config(
                            recipient_id=recipient_id,
                            share_pack_id=share_pack_id,
                            recipient_name=recipient_name,
                            databricks_recipient_id=result.name,  # Databricks recipient ID
                            recipient_contact_email=recip_config.get("recipient_contact_email", ""),
                            recipient_type=recipient_type,
                            recipient_databricks_org=recip_config.get("data_recipient_global_metastore_id"),
                            ip_access_list=recip_config.get("recipient_ips", []),
                            activation_url=result.activation_url if hasattr(result, "activation_url") else None,
                            bearer_token=None,  # Don't store tokens in DB
                            created_by="orchestrator",
                        )
                        created_db_records["recipients"].append(recipient_id)
                        logger.debug(f"Tracked recipient {recipient_name} in database (id: {recipient_id})")
                    except Exception as db_error:
                        logger.warning(f"Failed to track recipient {recipient_name} in database (UPDATE strategy - object should exist): {db_error}")

            # Update description if changed (support both 'description' and 'comment' for backward compatibility)
            new_description = recip_config.get("description") or recip_config.get("comment", "")
            if new_description and new_description != existing.comment:
                result = update_recipient_description(
                    recipient_name, new_description, workspace_url
                )
                if not isinstance(result, str) or "success" in result.lower():
                    logger.success(
                        f"Updated description for recipient: {recipient_name}"
                    )
                    updated_resources["recipients"].append(
                        f"{recipient_name} (description)"
                    )

            # Update IP access list for D2O recipients
            # Supports TWO approaches:
            # 1. Declarative: recipient_ips (complete list of desired IPs)
            # 2. Explicit: recipient_ips_to_add + recipient_ips_to_remove (incremental changes)
            if recipient_type == "D2O":
                has_declarative = "recipient_ips" in recip_config
                has_explicit_add = "recipient_ips_to_add" in recip_config
                has_explicit_remove = "recipient_ips_to_remove" in recip_config

                if has_declarative or has_explicit_add or has_explicit_remove:
                    # Get current IPs from Databricks
                    current_ips_in_databricks = (
                        set(existing.ip_access_list.allowed_ip_addresses)
                        if existing.ip_access_list
                        and existing.ip_access_list.allowed_ip_addresses
                        else set()
                    )
                    logger.debug(f"Current IPs in Databricks for {recipient_name}: {current_ips_in_databricks}")

                    ips_to_add = []
                    ips_to_remove = []

                    if has_declarative:
                        # APPROACH 1: Declarative - specify complete desired state
                        logger.info(f"Using DECLARATIVE approach for {recipient_name} IP management")
                        desired_ips_from_yaml = set(recip_config["recipient_ips"])
                        logger.debug(f"Desired IPs from YAML: {desired_ips_from_yaml}")

                        # Calculate differences
                        for ip in desired_ips_from_yaml:
                            if ip not in current_ips_in_databricks:
                                ips_to_add.append(ip)

                        for ip in current_ips_in_databricks:
                            if ip not in desired_ips_from_yaml:
                                ips_to_remove.append(ip)

                    else:
                        # APPROACH 2: Explicit - specify only changes
                        logger.info(f"Using EXPLICIT approach for {recipient_name} IP management")

                        if has_explicit_add:
                            explicit_add_list = recip_config.get("recipient_ips_to_add", [])
                            logger.debug(f"Explicit IPs to add: {explicit_add_list}")
                            # Only add IPs that don't already exist (idempotent)
                            for ip in explicit_add_list:
                                if ip not in current_ips_in_databricks:
                                    ips_to_add.append(ip)
                                else:
                                    logger.debug(f"IP {ip} already exists, skipping add")

                        if has_explicit_remove:
                            explicit_remove_list = recip_config.get("recipient_ips_to_remove", [])
                            logger.debug(f"Explicit IPs to remove: {explicit_remove_list}")
                            # Only remove IPs that actually exist (idempotent)
                            for ip in explicit_remove_list:
                                if ip in current_ips_in_databricks:
                                    ips_to_remove.append(ip)
                                else:
                                    logger.debug(f"IP {ip} doesn't exist, skipping remove")

                    # Execute IP additions
                    if ips_to_add:
                        logger.info(f"Adding {len(ips_to_add)} IP(s) to {recipient_name}: {ips_to_add}")
                        result = add_recipient_ip(
                            recipient_name, ips_to_add, workspace_url
                        )
                        if not isinstance(result, str) or "success" in result.lower():
                            logger.success(f"Successfully added {len(ips_to_add)} IP(s) to {recipient_name}")
                            updated_resources["recipients"].append(f"{recipient_name} (added IPs)")
                        else:
                            logger.error(f"Failed to add IPs to {recipient_name}: {result}")

                    # Execute IP removals
                    if ips_to_remove:
                        logger.info(f"Removing {len(ips_to_remove)} IP(s) from {recipient_name}: {ips_to_remove}")
                        result = revoke_recipient_ip(
                            recipient_name, ips_to_remove, workspace_url
                        )
                        if not isinstance(result, str) or "success" in result.lower():
                            logger.success(f"Successfully removed {len(ips_to_remove)} IP(s) from {recipient_name}")
                            updated_resources["recipients"].append(f"{recipient_name} (removed IPs)")
                        else:
                            logger.error(f"Failed to remove IPs from {recipient_name}: {result}")

                    # Log if no changes needed
                    if not ips_to_add and not ips_to_remove:
                        logger.debug(f"IP addresses for {recipient_name} are already up to date - no changes needed")

        except Exception as e:
            logger.error(f"Failed to update recipient {recipient_name}: {e}")
            # Continue with other recipients


async def _update_shares(
    workspace_url: str,
    shares: List[Dict],
    updated_resources: Dict,
    share_repo: ShareRepository,
    share_pack_id: UUID,
    created_db_records: Dict,
):
    """Update share configurations and track in database."""
    for share_config in shares:
        share_name = share_config["name"]

        logger.info(f"Checking share: {share_name}")

        try:
            # Get existing share
            existing = get_shares(share_name, workspace_url)

            if not existing:
                logger.info(f"Share {share_name} not found - creating new share")
                # Create new share
                result = create_share(
                    dltshr_workspace_url=workspace_url,
                    share_name=share_name,
                    description=share_config.get("description") or share_config.get("comment", ""),
                )

                if isinstance(result, str):
                    logger.error(f"Failed to create share {share_name}: {result}")
                    continue
                else:
                    logger.success(f"Created share: {share_name}")
                    updated_resources["shares"].append(f"{share_name} (created)")

                    # Track in database
                    try:
                        share_id = uuid4()
                        await share_repo.create_from_config(
                            share_id=share_id,
                            share_pack_id=share_pack_id,
                            share_name=share_name,
                            databricks_share_id=result.name,
                            description=share_config.get("description") or share_config.get("comment", ""),
                            storage_root="",
                            share_assets=share_config.get("share_assets", []),
                            recipients_attached=share_config.get("recipients", []),
                            created_by="orchestrator",
                        )
                        created_db_records["shares"].append(share_id)
                        logger.debug(f"Tracked share {share_name} in database (id: {share_id})")
                    except Exception as db_error:
                        logger.warning(f"Failed to track share {share_name} in database (UPDATE strategy - object should exist): {db_error}")
            else:
                # Share exists - nothing to update on the share itself
                # Main updates are data objects and permissions (handled separately)
                logger.debug(f"Share {share_name} exists")

        except Exception as e:
            logger.error(f"Failed to check share {share_name}: {e}")


async def _update_share_data_objects(
    workspace_url: str, shares: List[Dict], updated_resources: Dict
):
    """Update data objects in shares."""
    for share_config in shares:
        share_name = share_config["name"]

        if "share_assets" not in share_config:
            logger.debug(f"No share_assets specified for {share_name}, skipping")
            continue

        assets = share_config["share_assets"]
        logger.info(f"Updating {len(assets)} assets for share {share_name}")

        # Categorize assets by type
        tables_to_add = []
        schemas_to_add = []

        for asset in assets:
            parts = asset.split(".")
            if len(parts) == 1 or len(parts) == 2:
                schemas_to_add.append(asset)
            else:
                tables_to_add.append(asset)

        objects_to_add = {
            "tables": tables_to_add,
            "views": [],
            "schemas": schemas_to_add,
        }

        try:
            # Add data objects (API is idempotent - won't fail if already present)
            result = add_data_object_to_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                objects_to_add=objects_to_add,
            )

            if isinstance(result, str):
                if "already" in result.lower() or "duplicate" in result.lower():
                    logger.info(f"Assets already present in share {share_name}")
                else:
                    logger.error(f"Failed to add assets to {share_name}: {result}")
            else:
                logger.success(f"Updated data objects for share: {share_name}")
                updated_resources["data_objects"].append(share_name)

        except Exception as e:
            logger.error(f"Failed to update data objects for {share_name}: {e}")


async def _update_share_permissions(
    workspace_url: str, shares: List[Dict], updated_resources: Dict
):
    """Update recipient permissions on shares."""
    for share_config in shares:
        share_name = share_config["name"]

        if "recipients" not in share_config:
            logger.debug(f"No recipients specified for {share_name}, skipping")
            continue

        recipients = share_config["recipients"]
        logger.info(f"Updating {len(recipients)} recipient permissions for {share_name}")

        for recipient_name in recipients:
            try:
                # Add recipient to share (API is idempotent)
                result = add_recipients_to_share(
                    dltshr_workspace_url=workspace_url,
                    share_name=share_name,
                    recipient_name=recipient_name,
                )

                if isinstance(result, str):
                    if "already" in result.lower() or "duplicate" in result.lower():
                        logger.debug(
                            f"Recipient {recipient_name} already has access to {share_name}"
                        )
                    else:
                        logger.error(
                            f"Failed to attach {recipient_name} to {share_name}: {result}"
                        )
                else:
                    logger.success(
                        f"Updated permission: {recipient_name} → {share_name}"
                    )
                    updated_resources["permissions"].append(
                        f"{recipient_name} → {share_name}"
                    )

            except Exception as e:
                logger.error(
                    f"Failed to update permission {recipient_name} → {share_name}: {e}"
                )


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
                    logger.info(f"Pipeline {pipeline_name} not found - creating new pipeline")
                    # Create new pipeline
                    pipeline_id = await _create_pipeline(
                        workspace_url,
                        pipeline_name,
                        pipeline_config,
                        share_config,
                        updated_resources,
                        pipeline_repo,
                        share_repo,
                        share_pack_id,
                        created_db_records,
                    )
                    if not pipeline_id:
                        continue
                else:
                    logger.info(f"Found pipeline: {pipeline_name} (id: {pipeline_id})")

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
            logger.error(f"Missing source_asset for pipeline {pipeline_name}")
            return None

        # Default target_asset to source table name if not specified
        if not target_asset:
            target_asset = source_asset.split(".")[-1]

        # Get catalog and schema (pipeline-level overrides or share-level defaults)
        delta_share = share_config.get("delta_share", {})
        ext_catalog = pipeline_config.get("ext_catalog_name") or delta_share.get("ext_catalog_name")
        ext_schema = pipeline_config.get("ext_schema_name") or delta_share.get("ext_schema_name")

        if not ext_catalog or not ext_schema:
            logger.error(f"Missing catalog/schema for pipeline {pipeline_name}")
            return None

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
            logger.error(f"Failed to create pipeline {pipeline_name}: {result}")
            return None

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
                created_db_records["pipelines"].append(pipeline_id_db)
                logger.debug(f"Tracked pipeline {pipeline_name} in database (id: {pipeline_id_db})")
        except Exception as db_error:
            logger.warning(f"Failed to track pipeline {pipeline_name} in database (UPDATE strategy - object should exist): {db_error}")

        # Return Databricks pipeline_id
        return result.pipeline_id

    except Exception as e:
        logger.error(f"Failed to create pipeline {pipeline_name}: {e}")
        return None


async def _update_pipeline_configuration(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    pipeline_config: Dict,
    share_config: Dict,
    updated_resources: Dict,
):
    """Update pipeline configuration (target table, source, catalog, schema, etc.)."""
    try:
        # Extract configuration values
        source_asset = pipeline_config.get("source_asset")
        target_asset = pipeline_config.get("target_asset")

        if not source_asset and not target_asset:
            logger.debug(f"No source/target assets specified for {pipeline_name}, skipping config update")
            return

        # Default target_asset to source table name if not specified
        if not target_asset and source_asset:
            target_asset = source_asset.split(".")[-1]

        # Get catalog and schema (pipeline-level overrides or share-level defaults)
        delta_share = share_config.get("delta_share", {})
        ext_catalog = pipeline_config.get("ext_catalog_name") or delta_share.get("ext_catalog_name")
        ext_schema = pipeline_config.get("ext_schema_name") or delta_share.get("ext_schema_name")

        # Build configuration dictionary
        configuration = {
            "pipelines.target_table": target_asset,
            "pipelines.target_catalog": ext_catalog,
            "pipelines.target_schema": ext_schema,
        }

        # Add source_asset if present
        if source_asset:
            configuration["pipelines.source_table"] = source_asset

        # Add optional fields if present
        if "key_columns" in pipeline_config:
            configuration["pipelines.keys"] = pipeline_config["key_columns"]

        if "scd_type" in pipeline_config:
            configuration["pipelines.scd_type"] = pipeline_config["scd_type"]

        if "apply_changes" in pipeline_config:
            configuration["pipelines.apply_changes"] = str(pipeline_config["apply_changes"]).lower()

        logger.info(f"Updating pipeline configuration for {pipeline_name}")
        logger.debug(f"New configuration: {configuration}")

        # Get existing pipeline details to retrieve libraries (required by Databricks)
        from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name
        existing_pipeline = get_pipeline_by_name(workspace_url, pipeline_name)

        if isinstance(existing_pipeline, str):
            logger.error(f"Failed to get existing pipeline {pipeline_name}: {existing_pipeline}")
            return

        # Extract libraries from existing pipeline
        libraries = existing_pipeline.spec.libraries if existing_pipeline.spec else None
        if not libraries:
            logger.warning(f"No libraries found in existing pipeline {pipeline_name}, skipping config update")
            return

        # Update pipeline configuration
        result = update_pipeline_target_configuration(
            dltshr_workspace_url=workspace_url,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            configuration=configuration,
            catalog=ext_catalog,
            target=ext_schema,
            libraries=libraries,
            notifications=pipeline_config.get("notification"),
            tags=pipeline_config.get("tags"),
            serverless=pipeline_config.get("serverless"),
        )

        if isinstance(result, str):
            if "success" in result.lower() or "updated" in result.lower():
                logger.success(f"Updated configuration for pipeline: {pipeline_name}")
                updated_resources["pipelines"].append(f"{pipeline_name} (config)")
            else:
                logger.warning(f"Failed to update pipeline config for {pipeline_name}: {result}")
        else:
            logger.success(f"Updated configuration for pipeline: {pipeline_name}")
            updated_resources["pipelines"].append(f"{pipeline_name} (config)")

    except Exception as e:
        logger.error(f"Failed to update pipeline configuration for {pipeline_name}: {e}")


async def _update_pipeline_schedule(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    pipeline_config: Dict,
    updated_resources: Dict,
):
    """Update schedule for a specific pipeline."""
    schedule = pipeline_config.get("schedule")

    if not schedule:
        return

    # Get existing schedules for this pipeline
    try:
        schedules, _ = list_schedules(
            dltshr_workspace_url=workspace_url,
            pipeline_id=pipeline_id,
        )

        if not schedules:
            # No schedule exists - create one
            logger.info(f"No schedule found for {pipeline_name}, creating new one")
            await _create_schedule(
                workspace_url, pipeline_name, pipeline_id, schedule, pipeline_config
            )
            updated_resources["schedules"].append(f"{pipeline_name} (created)")
            return

        # Update existing schedule
        job_id = schedules[0]["job_id"]
        existing_cron = schedules[0].get("cron_schedule", {})

        if isinstance(schedule, dict):
            # Handle both v1.0 and v2.0 schedule formats
            new_cron = schedule.get("cron")
            new_timezone = schedule.get("timezone", "UTC")

            # v1.0 format: schedule has source_asset as key with nested cron/timezone
            if not new_cron:
                schedule_keys = [k for k in schedule.keys() if k not in ["cron", "timezone"]]
                if len(schedule_keys) == 1:
                    source_asset_key = schedule_keys[0]
                    nested_schedule = schedule[source_asset_key]
                    if isinstance(nested_schedule, dict):
                        new_cron = nested_schedule.get("cron")
                        new_timezone = nested_schedule.get("timezone", "UTC")
                        logger.info(f"[v1.0 FORMAT] Extracted cron from nested schedule for {pipeline_name}")

            # Check if cron changed
            if new_cron and existing_cron.get("cron_expression") != new_cron:
                result = update_schedule_for_pipeline(
                    dltshr_workspace_url=workspace_url,
                    job_id=job_id,
                    cron_expression=new_cron,
                )
                if "success" in result.lower():
                    logger.success(
                        f"Updated cron for {pipeline_name}: {new_cron}"
                    )
                    updated_resources["schedules"].append(
                        f"{pipeline_name} (cron)"
                    )

            # Check if timezone changed
            if existing_cron.get("timezone") != new_timezone:
                result = update_timezone_for_schedule(
                    dltshr_workspace_url=workspace_url,
                    job_id=job_id,
                    time_zone=new_timezone,
                )
                if "success" in result.lower():
                    logger.success(
                        f"Updated timezone for {pipeline_name}: {new_timezone}"
                    )
                    updated_resources["schedules"].append(
                        f"{pipeline_name} (timezone)"
                    )

    except Exception as e:
        logger.error(f"Failed to update schedule for {pipeline_name}: {e}")


async def _create_schedule(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    schedule: Any,
    pipeline_config: Dict,
):
    """Create a new schedule for a pipeline."""
    logger.info(f"Attempting to create schedule for {pipeline_name}, schedule type: {type(schedule)}, value: {schedule}")

    if isinstance(schedule, str) and schedule.lower() == "continuous":
        logger.warning(
            f"Continuous schedules not yet supported for {pipeline_name}"
        )
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
                            logger.warning(f"Job {job_name} exists but no active schedule found for pipeline - may need manual cleanup in Databricks Workflows")
                    except Exception as verify_error:
                        logger.warning(f"Could not verify schedule for {pipeline_name}: {verify_error}")
                elif "success" in result.lower() or "created" in result.lower():
                    logger.success(f"Created schedule for {pipeline_name} (job: {job_name}, cron: {cron_expression})")
                else:
                    logger.error(f"Failed to create schedule for {pipeline_name}: {result}")
            else:
                # Dict response - success
                logger.success(f"Created schedule for {pipeline_name} (job: {job_name})")
