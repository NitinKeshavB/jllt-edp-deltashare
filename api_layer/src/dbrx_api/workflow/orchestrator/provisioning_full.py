"""
Share Pack Provisioning - FULL Implementation

Implements complete provisioning with actual Databricks API calls.
"""

from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Dict

from loguru import logger

from dbrx_api.dbrx_auth.token_gen import get_auth_token
from dbrx_api.dltshr.recipient import create_recipient_d2d
from dbrx_api.dltshr.recipient import create_recipient_d2o
from dbrx_api.dltshr.share import add_data_object_to_share
from dbrx_api.dltshr.share import add_recipients_to_share
from dbrx_api.dltshr.share import create_share
from dbrx_api.jobs.dbrx_pipelines import create_pipeline
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

    # Track created resources for rollback
    created_resources = {
        "recipients": [],
        "shares": [],
        "pipelines": [],
    }

    try:
        config = share_pack["config"]  # Already parsed as dict from JSONB
        workspace_url = config["metadata"]["workspace_url"]

        logger.info(f"Starting NEW strategy provisioning for {share_pack_id}")
        logger.info(f"Target workspace: {workspace_url}")

        # Get session token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Step 1: Skip tenant/project resolution for MVP
        await tracker.update("Step 1/7: Initializing provisioning")
        logger.debug("Skipping tenant/project resolution for MVP")

        # Step 2: Create Recipients
        await tracker.update("Step 2/7: Creating recipients")
        recipient_results = {}

        for recip_config in config["recipient"]:
            recipient_name = recip_config["name"]
            recipient_type = recip_config["type"]

            logger.info(f"Creating {recipient_type} recipient: {recipient_name}")

            if recipient_type == "D2D":
                result = create_recipient_d2d(
                    workspace_url=workspace_url,
                    token=session_token,
                    recipient_name=recipient_name,
                    metastore_id=recip_config["recipient_databricks_org"],
                    comment=recip_config.get("comment", ""),
                )
            else:  # D2O
                result = create_recipient_d2o(
                    workspace_url=workspace_url,
                    token=session_token,
                    recipient_name=recipient_name,
                    allowed_ips=recip_config.get("recipient_ips", []),
                    comment=recip_config.get("comment", ""),
                )

            if isinstance(result, str):
                raise Exception(f"Failed to create recipient {recipient_name}: {result}")

            recipient_results[recipient_name] = result
            created_resources["recipients"].append(recipient_name)
            logger.success(f"Created recipient: {recipient_name}")

        # Step 3: Create Shares
        await tracker.update("Step 3/7: Creating shares")
        share_results = {}

        for share_config in config["share"]:
            share_name = share_config["name"]

            logger.info(f"Creating share: {share_name}")

            result = create_share(
                workspace_url=workspace_url,
                token=session_token,
                share_name=share_name,
                comment=share_config.get("comment", ""),
            )

            if isinstance(result, str):
                raise Exception(f"Failed to create share {share_name}: {result}")

            share_results[share_name] = result
            created_resources["shares"].append(share_name)
            logger.success(f"Created share: {share_name}")

        # Step 4: Add Data Objects to Shares
        await tracker.update("Step 4/7: Adding data objects to shares")

        for share_config in config["share"]:
            share_name = share_config["name"]
            assets = share_config["share_assets"]

            logger.info(f"Adding {len(assets)} assets to share {share_name}")

            # Add all assets to the share
            for asset in assets:
                # Determine object type based on parts count
                parts = asset.split(".")
                if len(parts) == 1:
                    object_type = "SCHEMA"
                elif len(parts) == 2:
                    object_type = "SCHEMA"
                else:  # 3 parts
                    object_type = "TABLE"

                result = add_data_object_to_share(
                    workspace_url=workspace_url,
                    token=session_token,
                    share_name=share_name,
                    data_object_name=asset,
                    data_object_type=object_type,
                )

                if isinstance(result, str):
                    raise Exception(f"Failed to add {asset} to {share_name}: {result}")

                logger.debug(f"Added {asset} to {share_name}")

            logger.success(f"Added all assets to share: {share_name}")

        # Step 5: Attach Recipients to Shares
        await tracker.update("Step 5/7: Attaching recipients to shares")

        for share_config in config["share"]:
            share_name = share_config["name"]
            recipients = share_config["recipients"]

            logger.info(f"Attaching {len(recipients)} recipients to share {share_name}")

            for recipient_name in recipients:
                result = add_recipients_to_share(
                    workspace_url=workspace_url,
                    token=session_token,
                    share_name=share_name,
                    recipient_name=recipient_name,
                )

                if isinstance(result, str):
                    raise Exception(f"Failed to attach {recipient_name} to {share_name}: {result}")

                logger.debug(f"Attached {recipient_name} to {share_name}")

            logger.success(f"Attached all recipients to share: {share_name}")

        # Step 6: Create DLT Pipelines
        await tracker.update("Step 6/7: Creating DLT pipelines")

        for share_config in config["share"]:
            share_name = share_config["name"]
            delta_share_config = share_config["delta_share"]
            pipelines = share_config.get("pipelines", [])

            logger.info(f"Creating {len(pipelines)} pipelines for share {share_name}")

            for pipeline_config in pipelines:
                pipeline_name = pipeline_config["name_prefix"]

                logger.info(f"Creating pipeline: {pipeline_name}")

                # Extract schedule info (first asset in schedule dict)
                schedule_dict = pipeline_config["schedule"]
                asset_name = list(schedule_dict.keys())[0]
                schedule_dict[asset_name]

                # Build configuration dictionary for create_pipeline
                configuration = {
                    "pipelines.source_table": f"{share_name}.default.{asset_name.split('.')[-1]}",
                    "pipelines.keys": pipeline_config.get("key_columns", ""),
                    "pipelines.scd_type": pipeline_config.get("scd_type", "2"),
                    "pipelines.target_table_prefix": delta_share_config.get("prefix_assetname", ""),
                }

                # Create pipeline
                result = create_pipeline(
                    dltshr_workspace_url=workspace_url,
                    pipeline_name=pipeline_name,
                    target_catalog_name=delta_share_config["ext_catalog_name"],
                    target_schema_name=delta_share_config["ext_schema_name"],
                    configuration=configuration,
                    notifications_list=pipeline_config.get("notification", []),
                    tags=pipeline_config.get("tags", {}),
                    serverless=pipeline_config.get("serverless", False),
                )

                if isinstance(result, str):
                    raise Exception(f"Failed to create pipeline {pipeline_name}: {result}")

                created_resources["pipelines"].append(pipeline_name)
                logger.success(f"Created pipeline: {pipeline_name}")

        # Step 7: Mark as completed
        await tracker.complete("Provisioning completed successfully")

        logger.success(f"Share pack {share_pack_id} provisioned successfully")
        logger.info(
            f"Created {len(created_resources['recipients'])} recipients, "
            f"{len(created_resources['shares'])} shares, "
            f"{len(created_resources['pipelines'])} pipelines"
        )

    except Exception as e:
        await tracker.fail(str(e))
        logger.error(f"Provisioning failed for {share_pack_id}: {e}", exc_info=True)

        # Log what was created before failure (for manual cleanup if needed)
        logger.warning(f"Resources created before failure: {created_resources}")

        raise
