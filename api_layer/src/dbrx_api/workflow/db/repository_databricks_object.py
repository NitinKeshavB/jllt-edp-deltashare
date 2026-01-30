"""
Databricks Object Repository

Repository for Databricks object CRUD operations with SCD Type 2 tracking.
"""

from uuid import UUID
import asyncpg
from dbrx_api.workflow.db.repository_base import BaseRepository


class DatabricksObjectRepository(BaseRepository):
    """Databricks object repository (synced from workspace)."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "databricks_objects", "object_id")

    async def get_by_full_name(self, workspace_url: str, full_name: str):
        """Get object by workspace URL and full name."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE workspace_url = $1 AND full_name = $2 AND is_current = true AND is_deleted = false
                """,
                workspace_url,
                full_name,
            )
            return dict(row) if row else None
