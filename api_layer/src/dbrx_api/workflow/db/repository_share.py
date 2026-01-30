"""
Share Repository

Repository for Delta Share CRUD operations with SCD Type 2 tracking.
"""

from typing import List, Dict, Any
from uuid import UUID
import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


class ShareRepository(BaseRepository):
    """Share repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "shares", "share_id")

    async def create_from_config(
        self,
        share_id: UUID,
        share_pack_id: UUID,
        share_name: str,
        databricks_share_id: str,
        description: str = "",
        storage_root: str = "",
        share_assets: List[str] = None,
        recipients_attached: List[str] = None,
        created_by: str = "orchestrator",
    ) -> UUID:
        """Create a new share from provisioning."""
        import json

        fields = {
            "share_pack_id": share_pack_id,
            "share_name": share_name,
            "databricks_share_id": databricks_share_id,
            "description": description,
            "storage_root": storage_root,
            "share_assets": json.dumps(share_assets or []),
            "recipients_attached": json.dumps(recipients_attached or []),
            "is_deleted": False,
        }

        return await self.create_or_update(
            share_id, fields, created_by, "Provisioned from share pack"
        )

    async def list_by_share_pack(
        self,
        share_pack_id: UUID,
    ) -> List[Dict[str, Any]]:
        """Get all shares for a share pack."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE share_pack_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY share_name
                """,
                share_pack_id,
            )
            return [dict(row) for row in rows]
