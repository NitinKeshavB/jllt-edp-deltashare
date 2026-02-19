"""
Pipeline cleanup logic for orphaned pipelines after share asset removal.

When assets are removed from shares, this module handles the cleanup of pipelines
that were processing those assets. It intelligently decides whether to delete
pipelines from Databricks based on whether the asset exists in other shares.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Set
from uuid import UUID

from dbrx_api.jobs.dbrx_pipelines import delete_pipeline
from dbrx_api.jobs.dbrx_schedule import delete_schedule_for_pipeline
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from loguru import logger


async def cleanup_orphaned_pipelines(
    share_pack_id: UUID,
    workspace_url: str,
    pipeline_repo: PipelineRepository,
    share_repo: ShareRepository,
) -> None:
    """
    Clean up pipelines whose source assets have been removed from shares.

    Logic:
    1. Find all pipelines for this share pack
    2. For each pipeline, check if its source_asset is still in the share's assets
    3. If not (orphaned):
       - Check if source_asset exists in ANY other share
       - If NO (asset not in other shares): Delete pipeline + schedule from Databricks
       - If YES (asset in other shares): Keep in Databricks, just soft-delete DB record
    4. Soft-delete the pipeline DB record in both cases

    Args:
        share_pack_id: UUID of the share pack being provisioned
        workspace_url: Databricks workspace URL
        pipeline_repo: Pipeline repository instance
        share_repo: Share repository instance
    """
    # Get all pipelines for this share pack
    pipelines = await pipeline_repo.list_by_share_pack(share_pack_id)
    if not pipelines:
        logger.debug(f"No pipelines found for share pack {share_pack_id}, skipping cleanup")
        return

    # Build mapping of share_id -> current assets
    # IMPORTANT: We need to look up the ACTUAL share for each pipeline's share_id,
    # not just shares in this share pack, because shares can be shared across share packs
    share_id_to_assets: Dict[UUID, Set[str]] = {}
    unique_share_ids = {p["share_id"] for p in pipelines if p.get("share_id")}

    for share_id in unique_share_ids:
        try:
            # Get current version of share (regardless of share_pack_id)
            current_share = await share_repo.get_current(
                share_id, include_deleted=False
            )
            if current_share:
                share_assets = current_share.get("share_assets") or []
                logger.info(
                    f"DEBUG: Share {share_id} found in DB. "
                    f"Raw share_assets type: {type(share_assets)}, "
                    f"value: {share_assets}"
                )
                # Parse JSON if needed
                if isinstance(share_assets, str):
                    import json
                    share_assets = (
                        json.loads(share_assets) if share_assets else []
                    )
                    logger.info(f"DEBUG: Parsed JSON: {share_assets}")
                share_id_to_assets[share_id] = set(share_assets)
                logger.info(
                    f"Share {share_id}: {len(share_assets)} assets - "
                    f"{share_assets}"
                )
            else:
                logger.warning(
                    f"Share {share_id} NOT found in DB. "
                    "This likely means pipelines have a stale share_id "
                    "from a deleted/recreated share. "
                    "SKIPPING cleanup for pipelines with this share_id "
                    "to prevent accidental deletion. "
                    "Manual cleanup may be required."
                )
                # DO NOT add to share_id_to_assets - skip these pipelines
        except Exception as e:
            logger.opt(exception=True).error(
                f"ERROR: Failed to get assets for share {share_id}: {e}"
            )
            # Do NOT add to share_id_to_assets on error - skip for safety

    # Get ALL shares (across all share packs) to check if asset exists elsewhere
    all_current_shares = await share_repo.list_all(include_deleted=False)
    asset_to_other_shares: Dict[str, List[str]] = {}
    for share_rec in all_current_shares:
        # Skip shares in the current share pack (we only care about OTHER shares)
        if share_rec.get("share_pack_id") == share_pack_id:
            continue
        share_name = share_rec.get("share_name")
        share_assets = share_rec.get("share_assets") or []
        for asset in share_assets:
            asset_to_other_shares.setdefault(asset, []).append(share_name)

    # Check each pipeline for orphaned status
    orphaned_pipelines: List[Dict[str, Any]] = []
    for pipeline_rec in pipelines:
        pipeline_id = pipeline_rec["pipeline_id"]
        pipeline_name = pipeline_rec["pipeline_name"]
        source_asset = pipeline_rec.get("source_table")
        share_id = pipeline_rec.get("share_id")
        databricks_pipeline_id = pipeline_rec.get("databricks_pipeline_id")

        if not source_asset or not share_id:
            logger.debug(
                f"Pipeline '{pipeline_name}' missing source_asset or "
                f"share_id, skipping"
            )
            continue

        # Check if we have share info for this share_id
        if share_id not in share_id_to_assets:
            logger.warning(
                f"Pipeline '{pipeline_name}': share_id {share_id} not "
                f"found in DB (likely stale). Skipping cleanup for safety. "
                f"Manual intervention may be required to fix share_id."
            )
            continue

        # Check if source_asset is still in the share's current assets
        current_share_assets = share_id_to_assets[share_id]
        if source_asset in current_share_assets:
            # Asset still exists in share, pipeline is still needed
            logger.debug(f"Pipeline '{pipeline_name}' asset '{source_asset}' still in share, keeping")
            continue

        # Asset removed from share - pipeline is orphaned
        # Check if asset exists in other shares
        other_shares_with_asset = asset_to_other_shares.get(source_asset, [])

        # CRITICAL: Check if OTHER database records reference same Databricks pipeline
        # (Shared pipeline scenario - multiple shares using same Databricks pipeline)
        should_delete_from_databricks = False
        if databricks_pipeline_id:
            try:
                # Get all DB records pointing to this Databricks pipeline
                all_pipeline_records = (
                    await pipeline_repo.list_by_databricks_pipeline_id(
                        databricks_pipeline_id, include_deleted=False
                    )
                )
                # Exclude the current record we're processing
                other_active_records = [
                    r
                    for r in all_pipeline_records
                    if r["pipeline_id"] != pipeline_id
                ]
                # Only delete from Databricks if NO other active records exist
                if len(other_active_records) == 0:
                    # No other DB records - safe to delete from Databricks
                    should_delete_from_databricks = True
                    logger.debug(
                        f"Pipeline '{pipeline_name}': No other DB records "
                        f"for databricks_pipeline_id={databricks_pipeline_id}"
                    )
                else:
                    logger.info(
                        f"Pipeline '{pipeline_name}': {len(other_active_records)} "
                        f"other DB record(s) reference "
                        f"databricks_pipeline_id={databricks_pipeline_id}. "
                        f"Keeping in Databricks."
                    )
            except Exception as check_err:
                logger.opt(exception=True).error(
                    f"Failed to check shared pipeline for '{pipeline_name}': "
                    f"{check_err}. Skipping Databricks deletion for safety."
                )
                should_delete_from_databricks = False
        else:
            logger.warning(
                f"Pipeline '{pipeline_name}' missing databricks_pipeline_id"
            )

        orphaned_pipelines.append(
            {
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "databricks_pipeline_id": databricks_pipeline_id,
                "source_asset": source_asset,
                "should_delete_from_databricks": should_delete_from_databricks,
                "other_shares": other_shares_with_asset,
            }
        )

    if not orphaned_pipelines:
        logger.info(f"No orphaned pipelines found for share pack {share_pack_id}")
        return

    logger.info(f"Found {len(orphaned_pipelines)} orphaned pipeline(s) to clean up")

    # Process each orphaned pipeline
    for orphan in orphaned_pipelines:
        pipeline_name = orphan["pipeline_name"]
        pipeline_id = orphan["pipeline_id"]
        databricks_pipeline_id = orphan["databricks_pipeline_id"]
        source_asset = orphan["source_asset"]
        should_delete = orphan["should_delete_from_databricks"]
        other_shares = orphan["other_shares"]

        try:
            if should_delete:
                # Asset not in other shares - safe to delete from Databricks
                logger.info(
                    f"Pipeline '{pipeline_name}': asset '{source_asset}' not in other shares, "
                    f"deleting from Databricks"
                )

                # Delete schedule first
                if databricks_pipeline_id:
                    try:
                        sch_result = delete_schedule_for_pipeline(
                            dltshr_workspace_url=workspace_url,
                            pipeline_id=databricks_pipeline_id,
                        )
                        if isinstance(sch_result, str):
                            if "no schedules found" in sch_result.lower() or "not found" in sch_result.lower():
                                logger.debug(f"No schedule found for pipeline '{pipeline_name}', skipping")
                            elif "error" in sch_result.lower():
                                logger.warning(f"Failed to delete schedule for '{pipeline_name}': {sch_result}")
                        else:
                            logger.info(f"Deleted schedule for pipeline '{pipeline_name}'")
                    except Exception as sch_err:
                        logger.warning(f"Failed to delete schedule for '{pipeline_name}': {sch_err}")

                    # Delete pipeline
                    try:
                        result = delete_pipeline(
                            dltshr_workspace_url=workspace_url,
                            pipeline_id=databricks_pipeline_id,
                        )
                        if result is not None:
                            logger.warning(f"Failed to delete pipeline '{pipeline_name}' from Databricks: {result}")
                        else:
                            logger.success(f"Deleted pipeline '{pipeline_name}' from Databricks")
                    except Exception as pipe_err:
                        logger.warning(f"Failed to delete pipeline '{pipeline_name}' from Databricks: {pipe_err}")
            else:
                # Asset exists in other shares - keep in Databricks
                logger.info(
                    f"Pipeline '{pipeline_name}': asset '{source_asset}' exists in other shares "
                    f"({', '.join(other_shares)}), keeping in Databricks but soft-deleting DB record"
                )

            # Soft-delete the pipeline DB record in both cases
            await pipeline_repo.soft_delete(
                entity_id=pipeline_id,
                deleted_by="orchestrator",
                deletion_reason=f"Asset '{source_asset}' removed from share (share pack {share_pack_id})",
                request_source="share_pack",
            )
            logger.info(f"Soft-deleted pipeline '{pipeline_name}' from database")

        except Exception as e:
            logger.opt(exception=True).error(f"Failed to clean up orphaned pipeline '{pipeline_name}': {e}")
            # Continue with other pipelines even if one fails


async def get_assets_being_removed(
    share_pack_id: UUID,
    shares_config: List[Dict[str, Any]],
    share_repo: ShareRepository,
) -> Dict[str, Set[str]]:
    """
    Determine which assets are being removed from each share.

    Compares current share assets in DB with the desired state from config
    to identify assets that will be removed.

    Args:
        share_pack_id: UUID of the share pack
        shares_config: List of share configurations from YAML
        share_repo: Share repository instance

    Returns:
        Dict mapping share_name -> set of assets being removed
    """
    assets_to_remove: Dict[str, Set[str]] = {}

    # Get current shares from DB
    current_shares = await share_repo.list_by_share_pack(share_pack_id)
    current_shares_map = {rec["share_name"]: rec for rec in current_shares}

    for share_config in shares_config:
        share_name = share_config["name"]

        # Determine desired assets using same logic as share_flow.py
        share_assets_declarative = share_config.get("share_assets", [])
        share_assets_to_add_explicit = share_config.get("share_assets_to_add", [])
        share_assets_to_remove_explicit = share_config.get("share_assets_to_remove", [])

        if share_assets_to_remove_explicit:
            # Explicit removal specified
            assets_to_remove[share_name] = set(share_assets_to_remove_explicit)
        elif share_assets_declarative:
            # Declarative approach - compute diff
            current_record = current_shares_map.get(share_name)
            if current_record:
                current_assets = set(current_record.get("share_assets") or [])
                desired_assets = set(share_assets_declarative)
                removed = current_assets - desired_assets
                if removed:
                    assets_to_remove[share_name] = removed

    return assets_to_remove
