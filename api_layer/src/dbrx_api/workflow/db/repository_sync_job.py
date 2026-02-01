"""
Sync Job Repository

Repository for sync job operations (append-only table).
"""

from typing import Optional
from uuid import UUID
from uuid import uuid4

import asyncpg


class SyncJobRepository:
    """Sync job repository (append-only, no SCD2)."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(
        self,
        sync_type: str,
        workspace_url: Optional[str] = None,
    ) -> UUID:
        """Create a new sync job (RUNNING status)."""
        sync_job_id = uuid4()

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO deltashare.sync_jobs
                    (sync_job_id, sync_type, workspace_url, status, started_at)
                VALUES ($1, $2, $3, 'RUNNING', NOW())
                """,
                sync_job_id,
                sync_type,
                workspace_url,
            )

        return sync_job_id

    async def complete(
        self,
        sync_job_id: UUID,
        records_processed: int = 0,
        records_created: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
    ) -> None:
        """Mark sync job as COMPLETED."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE deltashare.sync_jobs
                SET status = 'COMPLETED',
                    completed_at = NOW(),
                    records_processed = $2,
                    records_created = $3,
                    records_updated = $4,
                    records_failed = $5
                WHERE sync_job_id = $1
                """,
                sync_job_id,
                records_processed,
                records_created,
                records_updated,
                records_failed,
            )

    async def fail(
        self,
        sync_job_id: UUID,
        error_message: str,
    ) -> None:
        """Mark sync job as FAILED."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE deltashare.sync_jobs
                SET status = 'FAILED',
                    completed_at = NOW(),
                    error_message = $2
                WHERE sync_job_id = $1
                """,
                sync_job_id,
                error_message,
            )
