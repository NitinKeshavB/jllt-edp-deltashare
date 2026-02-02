"""
Share Pack Provisioning - FULL Implementation

Implements complete provisioning with actual Databricks API calls.
Tracks all resources in database with rollback support.
"""

from typing import Any, Dict
from uuid import uuid4

from loguru import logger

from dbrx_api.workflow.db.repository_recipient import RecipientRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository
from dbrx_api.dltshr.recipient import create_recipient_d2d, create_recipient_d2o
from dbrx_api.dltshr.share import (
    add_data_object_to_share,
    add_recipients_to_share,
    create_share,
)
from dbrx_api.jobs.dbrx_pipelines import create_pipeline, list_pipelines_with_search_criteria
from dbrx_api.workflow.orchestrator.status_tracker import StatusTracker


async def provision_sharepack_new(pool, share_pack: Dict[str, Any]):
    """
    Provision a share pack using NEW strategy (create all entities from scratch).

    This implementation calls actual Databricks APIs.

    Args:
        pool: asyncpg connection pool
        share_pack: Share pack dict from database (includes config as JSONB)

    Raises:
        Exception: If provisioning fails
    """
    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)

    # Initialize repositories
    recipient_repo = RecipientRepository(pool)
    share_repo = ShareRepository(pool)
    pipeline_repo = PipelineRepository(pool)

    # Track created resources for rollback
    created_resources = {
        "recipients": [],
        "shares": [],
        "pipelines": [],
    }

    # Track database IDs for rollback
    created_db_records = {
        "recipients": [],
        "shares": [],
        "pipelines": [],
    }

    try:
        import json

        # Parse config if it's a JSON string
        config = share_pack["config"]
        if isinstance(config, str):
            config = json.loads(config)

        workspace_url = config["metadata"]["workspace_url"]

        logger.info(f"Starting NEW strategy provisioning for {share_pack_id}")
        logger.info(f"Target workspace: {workspace_url}")

        # Step 1: Skip tenant/project resolution for MVP
        await tracker.update("Step 1/8: Initializing provisioning")
        logger.debug("Skipping tenant/project resolution for MVP")

        # Step 2: Create Recipients
        await tracker.update("Step 2/8: Creating recipients")
        recipient_results = {}

        for recip_config in config["recipient"]:
            recipient_name = recip_config["name"]
            recipient_type = recip_config["type"]

            logger.info(f"Creating {recipient_type} recipient: {recipient_name}")

            try:
                description_value = recip_config.get("description") or recip_config.get("comment", "")
                logger.info(f"Recipient {recipient_name} description from config: '{description_value}'")
                logger.debug(f"Full recip_config keys: {list(recip_config.keys())}")

                if recipient_type == "D2D":
                    result = create_recipient_d2d(
                        recipient_name=recipient_name,
                        recipient_identifier=recip_config["recipient_databricks_org"],
                        description=description_value,
                        dltshr_workspace_url=workspace_url,
                    )
                else:  # D2O
                    ip_list = recip_config.get("recipient_ips", [])
                    logger.info(f"Creating D2O recipient {recipient_name} with IPs: {ip_list}")
                    result = create_recipient_d2o(
                        recipient_name=recipient_name,
                        description=description_value,
                        dltshr_workspace_url=workspace_url,
                        ip_access_list=ip_list,
                    )

                if isinstance(result, str):
                    # Check if resource already exists (idempotent behavior)
                    error_lower = result.lower()
                    if any(keyword in error_lower for keyword in ["already exists", "already present", "duplicate"]):
                        logger.warning(f"Recipient {recipient_name} already exists, skipping creation")
                        created_resources["recipients"].append(f"{recipient_name} (existing)")
                    else:
                        raise Exception(f"Failed to create recipient {recipient_name}: {result}")
                else:
                    recipient_results[recipient_name] = result
                    created_resources["recipients"].append(recipient_name)
                    logger.success(f"Created recipient: {recipient_name}")

                    # Verify and add IPs if needed (for D2O recipients)
                    if recipient_type == "D2O" and ip_list:
                        # Check if IPs were actually applied during creation
                        actual_ips = (
                            set(result.ip_access_list.allowed_ip_addresses)
                            if hasattr(result, "ip_access_list")
                            and result.ip_access_list
                            and result.ip_access_list.allowed_ip_addresses
                            else set()
                        )
                        expected_ips = set(ip_list)

                        logger.info(f"IP verification for {recipient_name}:")
                        logger.info(f"  Expected IPs: {expected_ips}")
                        logger.info(f"  Actual IPs: {actual_ips}")

                        missing_ips = expected_ips - actual_ips
                        if missing_ips:
                            logger.warning(f"IPs not applied during creation, adding them explicitly: {missing_ips}")
                            from dbrx_api.dltshr.recipient import add_recipient_ip

                            add_result = add_recipient_ip(
                                recipient_name=recipient_name,
                                ip_access_list=list(missing_ips),
                                dltshr_workspace_url=workspace_url
                            )

                            if isinstance(add_result, str) and "error" in add_result.lower():
                                logger.error(f"Failed to add IPs after creation: {add_result}")
                            else:
                                logger.success(f"Successfully added {len(missing_ips)} missing IPs to {recipient_name}")

                    # Track in database
                    try:
                        recipient_id = uuid4()
                        await recipient_repo.create_from_config(
                            recipient_id=recipient_id,
                            share_pack_id=share_pack_id,
                            recipient_name=recipient_name,
                            databricks_recipient_id=result.name,
                            recipient_contact_email=recip_config.get("recipient_contact_email", ""),
                            recipient_type=recipient_type,
                            recipient_databricks_org=recip_config.get("data_recipient_global_metastore_id")
                            if recipient_type == "D2D"
                            else None,
                            ip_access_list=recip_config.get("recipient_ips", [])
                            if recipient_type == "D2O"
                            else [],
                            activation_url=result.activation_url if hasattr(result, "activation_url") else None,
                            bearer_token=None,
                            created_by="orchestrator",
                        )
                        created_db_records["recipients"].append(recipient_id)
                        logger.debug(f"Tracked recipient {recipient_name} in database (id: {recipient_id})")
                    except Exception as db_error:
                        logger.info(f"Failed to track recipient {recipient_name} in database (NEW strategy - expected for new objects): {db_error}")
            except Exception as e:
                # Catch any exception and check if it's an "already exists" type error
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ["already exists", "already present", "duplicate"]):
                    logger.warning(f"Recipient {recipient_name} already exists (caught exception), skipping creation")
                    created_resources["recipients"].append(f"{recipient_name} (existing)")
                else:
                    # Re-raise if it's a different type of error
                    raise

        # Step 3: Create Shares
        await tracker.update("Step 3/8: Creating shares")
        share_results = {}

        for share_config in config["share"]:
            share_name = share_config["name"]

            logger.info(f"Creating share: {share_name}")

            try:
                share_description = share_config.get("description") or share_config.get("comment", "")
                logger.info(f"Share {share_name} description from config: '{share_description}'")
                logger.debug(f"Full share_config keys: {list(share_config.keys())}")

                result = create_share(
                    dltshr_workspace_url=workspace_url,
                    share_name=share_name,
                    description=share_description,
                )

                if isinstance(result, str):
                    # Check if resource already exists (idempotent behavior)
                    error_lower = result.lower()
                    if any(keyword in error_lower for keyword in ["already exists", "already present", "duplicate"]):
                        logger.warning(f"Share {share_name} already exists, skipping creation")
                        created_resources["shares"].append(f"{share_name} (existing)")
                    else:
                        raise Exception(f"Failed to create share {share_name}: {result}")
                else:
                    share_results[share_name] = result
                    created_resources["shares"].append(share_name)
                    logger.success(f"Created share: {share_name}")

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
                        logger.info(f"Failed to track share {share_name} in database (NEW strategy - expected for new objects): {db_error}")
            except Exception as e:
                # Catch any exception and check if it's an "already exists" type error
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ["already exists", "already present", "duplicate"]):
                    logger.warning(f"Share {share_name} already exists (caught exception), skipping creation")
                    created_resources["shares"].append(f"{share_name} (existing)")
                else:
                    # Re-raise if it's a different type of error
                    raise

        # Step 4: Add Data Objects to Shares
        await tracker.update("Step 4/8: Adding data objects to shares")

        for share_config in config["share"]:
            share_name = share_config["name"]
            assets = share_config["share_assets"]

            logger.info(f"Adding {len(assets)} assets to share {share_name}")

            # Categorize assets by type
            tables_to_add = []
            schemas_to_add = []

            for asset in assets:
                # Determine object type based on parts count
                parts = asset.split(".")
                if len(parts) == 1 or len(parts) == 2:
                    # 1 or 2 parts = schema (catalog.schema or just schema)
                    schemas_to_add.append(asset)
                else:
                    # 3 parts = table (catalog.schema.table)
                    tables_to_add.append(asset)

            # Build objects_to_add dict
            objects_to_add = {
                "tables": tables_to_add,
                "views": [],  # Not used in current implementation
                "schemas": schemas_to_add,
            }

            result = add_data_object_to_share(
                dltshr_workspace_url=workspace_url,
                share_name=share_name,
                objects_to_add=objects_to_add,
            )

            if isinstance(result, str):
                # Check if error is due to already existing objects (idempotent behavior)
                error_lower = result.lower()
                if "already" in error_lower or "duplicate" in error_lower:
                    logger.warning(f"Some assets already exist in share {share_name}, continuing")
                else:
                    raise Exception(f"Failed to add assets to {share_name}: {result}")
            else:
                logger.success(f"Added all assets to share: {share_name}")

        # Step 5: Attach Recipients to Shares
        await tracker.update("Step 5/8: Attaching recipients to shares")

        for share_config in config["share"]:
            share_name = share_config["name"]
            recipients = share_config["recipients"]

            logger.info(f"Attaching {len(recipients)} recipients to share {share_name}")

            for recipient_name in recipients:
                result = add_recipients_to_share(
                    dltshr_workspace_url=workspace_url,
                    share_name=share_name,
                    recipient_name=recipient_name,
                )

                if isinstance(result, str):
                    # Check if error is due to already attached recipient (idempotent behavior)
                    error_lower = result.lower()
                    if "already" in error_lower or "duplicate" in error_lower:
                        logger.warning(f"Recipient {recipient_name} already attached to {share_name}, skipping")
                    else:
                        raise Exception(f"Failed to attach {recipient_name} to {share_name}: {result}")
                else:
                    logger.debug(f"Attached {recipient_name} to {share_name}")

            logger.success(f"Attached all recipients to share: {share_name}")

        # Step 6: Create DLT Pipelines
        await tracker.update("Step 6/8: Creating DLT pipelines")

        # Track pipelines for schedule creation in Step 7
        pipelines_for_scheduling = []

        for share_config in config["share"]:
            share_name = share_config["name"]
            delta_share_config = share_config["delta_share"]
            pipelines = share_config.get("pipelines", [])

            logger.info(f"Creating {len(pipelines)} pipelines for share {share_name}")

            for pipeline_config in pipelines:
                pipeline_name = pipeline_config["name_prefix"]
                logger.debug(f"Pipeline config keys for {pipeline_name}: {list(pipeline_config.keys())}")
                logger.debug(f"Schedule present: {'schedule' in pipeline_config}, value: {pipeline_config.get('schedule')}")

                # BACKWARDS COMPATIBILITY: Extract source_asset from v1.0 or v2.0 format
                source_asset = pipeline_config.get("source_asset")

                if source_asset is None:
                    # v1.0 format: extract from schedule dict
                    schedule = pipeline_config.get("schedule", {})
                    if isinstance(schedule, dict):
                        schedule_keys = [k for k in schedule.keys() if k not in ["cron", "timezone"]]
                        if len(schedule_keys) == 1:
                            source_asset = schedule_keys[0]
                            logger.warning(
                                f"[MIGRATION] Pipeline '{pipeline_name}': Extracted source_asset='{source_asset}' "
                                f"from v1.0 schedule format. Consider upgrading to v2.0 format."
                            )
                        else:
                            raise Exception(
                                f"Pipeline '{pipeline_name}': Cannot determine source_asset. "
                                f"Please add explicit source_asset field."
                            )
                    else:
                        raise Exception(
                            f"Pipeline '{pipeline_name}': source_asset is required. "
                            f"Use v2.0 format with explicit source_asset field."
                        )

                # Get target_asset from pipeline config
                target_asset = pipeline_config.get("target_asset")
                if not target_asset:
                    # Default: use source_asset name if target not specified
                    target_asset = source_asset.split(".")[-1] if source_asset else ""

                logger.info(f"Creating pipeline: {pipeline_name} (source: {source_asset}, target: {target_asset})")

                # Build configuration dictionary for create_pipeline
                configuration = {
                    "pipelines.source_table": source_asset,
                    "pipelines.target_table": target_asset,
                    "pipelines.keys": pipeline_config.get("key_columns", ""),
                    "pipelines.scd_type": pipeline_config.get("scd_type", "2"),
                }

                # Determine target catalog and schema
                # Priority: pipeline-level config > delta_share-level config
                target_catalog = pipeline_config.get("ext_catalog_name") or delta_share_config["ext_catalog_name"]
                target_schema = pipeline_config.get("ext_schema_name") or delta_share_config["ext_schema_name"]

                logger.debug(
                    f"Pipeline {pipeline_name} target: {target_catalog}.{target_schema} "
                    f"(override: {bool(pipeline_config.get('ext_catalog_name'))})"
                )

                # Create pipeline
                pipeline_id = None
                pipeline_was_created = False

                try:
                    result = create_pipeline(
                        dltshr_workspace_url=workspace_url,
                        pipeline_name=pipeline_name,
                        target_catalog_name=target_catalog,
                        target_schema_name=target_schema,
                        configuration=configuration,
                        notifications_list=pipeline_config.get("notification", []),
                        tags=pipeline_config.get("tags", {}),
                        serverless=pipeline_config.get("serverless", False),
                    )

                    if isinstance(result, str):
                        # Check if resource already exists (idempotent behavior)
                        error_lower = result.lower()
                        if any(keyword in error_lower for keyword in ["already exists", "already present", "duplicate"]):
                            logger.warning(f"Pipeline {pipeline_name} already exists, skipping creation")
                            created_resources["pipelines"].append(f"{pipeline_name} (existing)")
                        else:
                            raise Exception(f"Failed to create pipeline {pipeline_name}: {result}")
                    else:
                        # Pipeline created successfully - extract pipeline_id
                        pipeline_id = result.pipeline_id
                        pipeline_was_created = True
                        created_resources["pipelines"].append(pipeline_name)
                        logger.success(f"Created pipeline: {pipeline_name} (id: {pipeline_id})")

                        # Track in database
                        try:
                            # Get share_id from database
                            share_records = await share_repo.list_by_share_pack(share_pack_id)
                            share_id = None
                            for share_record in share_records:
                                if share_record["share_name"] == share_name:
                                    share_id = share_record["share_id"]
                                    break

                            if share_id:
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
                                    databricks_pipeline_id=pipeline_id,
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
                            else:
                                logger.warning(f"Could not find share_id for {share_name}, skipping pipeline database tracking")
                        except Exception as db_error:
                            logger.info(f"Failed to track pipeline {pipeline_name} in database (NEW strategy - expected for new objects): {db_error}")
                except Exception as e:
                    # Catch any exception and check if it's an "already exists" type error
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ["already exists", "already present", "duplicate"]):
                        logger.warning(f"Pipeline {pipeline_name} already exists (caught exception), skipping creation")
                        created_resources["pipelines"].append(f"{pipeline_name} (existing)")
                    else:
                        # Re-raise if it's a different type of error
                        raise

                # Get pipeline_id if pipeline already existed
                if pipeline_id is None:
                    # Pipeline already existed - look up its ID
                    try:
                        pipelines = list_pipelines_with_search_criteria(
                            dltshr_workspace_url=workspace_url,
                            filter_expr=pipeline_name,
                        )
                        for pipeline in pipelines:
                            if pipeline.name == pipeline_name:
                                pipeline_id = pipeline.pipeline_id
                                logger.debug(f"Found existing pipeline ID: {pipeline_id}")
                                break
                    except Exception as e:
                        logger.warning(f"Could not look up pipeline ID for {pipeline_name}: {e}")

                # Collect pipeline info for schedule creation in Step 7
                if pipeline_id and "schedule" in pipeline_config and pipeline_config.get("schedule"):
                    pipelines_for_scheduling.append({
                        "pipeline_name": pipeline_name,
                        "pipeline_id": pipeline_id,
                        "pipeline_config": pipeline_config,
                    })
                    logger.debug(f"Pipeline {pipeline_name} queued for schedule creation in Step 7")

        # Step 7: Create Pipeline Schedules
        await tracker.update("Step 7/8: Creating pipeline schedules")
        logger.info(f"Creating schedules for {len(pipelines_for_scheduling)} pipelines")

        schedules_created = 0
        for pipeline_info in pipelines_for_scheduling:
            pipeline_name = pipeline_info["pipeline_name"]
            pipeline_id = pipeline_info["pipeline_id"]
            pipeline_config = pipeline_info["pipeline_config"]
            schedule = pipeline_config.get("schedule")

            job_name = f"{pipeline_name}_schedule"
            logger.info(f"Creating schedule for pipeline: {pipeline_name} (job: {job_name})")

            try:
                # Handle different schedule formats
                if isinstance(schedule, str):
                    # Continuous schedule
                    if schedule.lower() == "continuous":
                        logger.warning(
                            f"Continuous schedule requested for {pipeline_name}, "
                            f"but continuous jobs are not yet implemented. Skipping schedule creation."
                        )
                    else:
                        logger.warning(f"Unknown schedule format for {pipeline_name}: {schedule}")
                elif isinstance(schedule, dict):
                    # Cron schedule - handle both v1.0 and v2.0 formats
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

                    logger.debug(f"Extracted from schedule dict - cron: {cron_expression}, timezone: {timezone}")

                    if cron_expression:
                        from dbrx_api.jobs.dbrx_schedule import create_schedule_for_pipeline, list_schedules

                        schedule_description = pipeline_config.get("description")
                        logger.info(f"Schedule {job_name} description from config: '{schedule_description}'")
                        logger.debug(f"Full pipeline_config keys: {list(pipeline_config.keys())}")

                        schedule_result = create_schedule_for_pipeline(
                            dltshr_workspace_url=workspace_url,
                            job_name=job_name,
                            pipeline_id=pipeline_id,
                            cron_expression=cron_expression,
                            time_zone=timezone,
                            paused=False,
                            email_notifications=pipeline_config.get("notification", []),
                            tags=pipeline_config.get("tags", {}),
                            description=schedule_description,
                        )

                        if isinstance(schedule_result, str):
                            if "already exists" in schedule_result.lower():
                                logger.info(f"Job {job_name} already exists - verifying schedule is active")
                                # Verify the existing schedule is properly configured
                                try:
                                    schedules, _ = list_schedules(workspace_url, pipeline_id)
                                    if schedules:
                                        logger.success(f"Schedule for {pipeline_name} exists and is active (job: {job_name})")
                                        schedules_created += 1
                                    else:
                                        logger.warning(f"Job {job_name} exists but no active schedule found for pipeline {pipeline_id} - may need manual cleanup in Databricks Workflows")
                                except Exception as verify_error:
                                    logger.warning(f"Could not verify schedule for {pipeline_name}: {verify_error}")
                            elif "successfully" in schedule_result.lower() or "created" in schedule_result.lower():
                                logger.success(f"Created schedule: {job_name} (cron: {cron_expression}, tz: {timezone})")
                                schedules_created += 1
                            else:
                                logger.error(f"Failed to create schedule for {pipeline_name}: {schedule_result}")
                        else:
                            # Dict response - success
                            logger.success(f"Created schedule: {job_name} (cron: {cron_expression}, tz: {timezone})")
                            schedules_created += 1
                    else:
                        logger.warning(f"No cron expression found in schedule for {pipeline_name}")
                else:
                    logger.warning(f"Unexpected schedule format for {pipeline_name}: {type(schedule)}")
            except Exception as e:
                # Log schedule creation failure but don't fail provisioning
                logger.error(f"Failed to create schedule for {pipeline_name}: {e}", exc_info=True)
                logger.warning(f"Continuing provisioning despite schedule creation failure")

        logger.info(f"Schedule creation complete: {schedules_created}/{len(pipelines_for_scheduling)} schedules created/verified")

        # Step 8: Mark as completed
        await tracker.complete()

        logger.success(f"Share pack {share_pack_id} provisioned successfully")
        logger.info(f"Created {len(created_resources['recipients'])} recipients, "
                   f"{len(created_resources['shares'])} shares, "
                   f"{len(created_resources['pipelines'])} pipelines, "
                   f"{schedules_created} schedules")

    except Exception as e:
        await tracker.fail(str(e))
        logger.error(f"Provisioning failed for {share_pack_id}: {e}", exc_info=True)
        logger.warning(f"Resources created before failure: {created_resources}")

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
