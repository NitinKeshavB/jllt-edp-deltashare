"""
AD Group Repository

Repository for Azure AD group CRUD operations with SCD Type 2 tracking.
"""

from uuid import UUID
import asyncpg
from dbrx_api.workflow.db.repository_base import BaseRepository


class ADGroupRepository(BaseRepository):
    """AD Group repository (synced from Azure AD)."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "ad_groups", "group_id")

    async def get_by_name(self, group_name: str):
        """Get group by name."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE group_name = $1 AND is_current = true AND is_deleted = false
                """,
                group_name,
            )
            return dict(row) if row else None
