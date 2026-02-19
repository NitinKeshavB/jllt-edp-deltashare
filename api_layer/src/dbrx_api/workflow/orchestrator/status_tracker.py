"""
Status Tracker

Helper for updating share pack status during provisioning.
"""

from uuid import UUID

from loguru import logger


class StatusTracker:
    """
    Helper for updating share pack provisioning status.

    Wraps repository calls to update status and log progress.
    """

    def __init__(self, pool, share_pack_id: UUID):
        """
        Initialize status tracker.

        Args:
            pool: asyncpg connection pool
            share_pack_id: Share pack UUID being provisioned
        """
        self.pool = pool
        self.share_pack_id = share_pack_id

    async def update(self, message: str):
        """
        Update provisioning status (IN_PROGRESS).

        Args:
            message: Status message
        """
        from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

        repo = SharePackRepository(self.pool)
        await repo.update_status(
            self.share_pack_id, "IN_PROGRESS", provisioning_status=message, updated_by="orchestrator"
        )
        logger.info(f"[{self.share_pack_id}] {message}")

    async def complete(self, message: str = "All steps completed successfully"):
        """
        Mark share pack as COMPLETED.

        Args:
            message: Status message (default: "All steps completed successfully")
        """
        from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

        repo = SharePackRepository(self.pool)
        await repo.update_status(
            self.share_pack_id,
            "COMPLETED",
            provisioning_status=message,
            updated_by="orchestrator",
        )
        logger.success(f"[{self.share_pack_id}] Provisioning COMPLETED: {message}")

    async def fail(self, error: str, provisioning_status: str = "Provisioning failed"):
        """
        Mark share pack as FAILED.

        Args:
            error: Error message
            provisioning_status: Optional status message (e.g. step where failed); defaults to
                "Provisioning failed" so FAILED row has an explicit message.
        """
        from dbrx_api.workflow.db.repository_share_pack import SharePackRepository

        repo = SharePackRepository(self.pool)
        await repo.update_status(
            self.share_pack_id,
            "FAILED",
            provisioning_status=provisioning_status,
            error_message=error,
            updated_by="orchestrator",
        )
        logger.error(f"[{self.share_pack_id}] Provisioning FAILED: {error}")

        # For MVP, skip notification system
        # In full implementation:
        # from dbrx_api.workflow.sync.notification_sender import send_failure_notification
        # await send_failure_notification(self.pool, self.share_pack_id, error)
