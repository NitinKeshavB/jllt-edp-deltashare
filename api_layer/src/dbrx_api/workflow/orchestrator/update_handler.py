"""
Share Pack Update Handler - UPDATE Strategy

Implements the UPDATE strategy for share pack provisioning.
For MVP, this is a simplified stub.
"""

from typing import Dict, Any
from loguru import logger

from dbrx_api.workflow.orchestrator.status_tracker import StatusTracker


async def provision_sharepack_update(pool, share_pack: Dict[str, Any]):
    """
    Provision a share pack using UPDATE strategy (diff + apply changes).

    For MVP, this is a stub that delegates to NEW strategy.
    Production implementation would:
    1. Fetch existing state from Databricks
    2. Diff with desired state
    3. Apply only necessary changes

    Args:
        pool: asyncpg connection pool
        share_pack: Share pack dict from database

    Raises:
        Exception: If provisioning fails
    """
    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)

    try:
        logger.info(f"Starting UPDATE strategy provisioning for {share_pack_id} (MVP: delegates to NEW)")

        await tracker.update("UPDATE strategy - using NEW strategy for MVP")

        # For MVP, delegate to NEW strategy
        from dbrx_api.workflow.orchestrator.provisioning import provision_sharepack_new
        await provision_sharepack_new(pool, share_pack)

        # Production implementation would:
        # 1. Fetch current state from Databricks (shares, recipients, etc.)
        # 2. Compare with desired state from config
        # 3. Apply only delta operations (add/remove/modify)

    except Exception as e:
        logger.error(f"UPDATE provisioning failed for {share_pack_id}: {e}", exc_info=True)
        await tracker.fail(str(e))
        raise
