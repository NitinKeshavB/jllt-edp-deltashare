"""
Pipeline Repository

Repository for Databricks pipeline CRUD operations with SCD Type 2 tracking.
"""

from typing import List, Dict, Any
from uuid import UUID
import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


class PipelineRepository(BaseRepository):
    """Pipeline repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "pipelines", "pipeline_id")

    async def create_from_config(
        self,
        pipeline_id: UUID,
        share_id: UUID,
        share_pack_id: UUID,
        pipeline_name: str,
        databricks_pipeline_id: str,
        asset_name: str,
        source_table: str,
        target_table: str,
        scd_type: str = "2",
        key_columns: str = "",
        schedule_type: str = "CRON",
        cron_expression: str = "",
        timezone: str = "UTC",
        serverless: bool = False,
        tags: Dict[str, str] = None,
        notification_emails: List[str] = None,
        created_by: str = "orchestrator",
    ) -> UUID:
        """Create a new pipeline from provisioning."""
        import json

        fields = {
            "share_id": share_id,
            "share_pack_id": share_pack_id,
            "pipeline_name": pipeline_name,
            "databricks_pipeline_id": databricks_pipeline_id,
            "asset_name": asset_name,
            "source_table": source_table,
            "target_table": target_table,
            "scd_type": scd_type,
            "key_columns": key_columns,
            "schedule_type": schedule_type,
            "cron_expression": cron_expression,
            "timezone": timezone,
            "serverless": serverless,
            "tags": json.dumps(tags or {}),
            "notification_emails": json.dumps(notification_emails or []),
            "is_deleted": False,
        }

        return await self.create_or_update(
            pipeline_id, fields, created_by, "Provisioned from share pack"
        )

    async def list_by_share_pack(
        self,
        share_pack_id: UUID,
    ) -> List[Dict[str, Any]]:
        """Get all pipelines for a share pack."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE share_pack_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY pipeline_name
                """,
                share_pack_id,
            )
            return [dict(row) for row in rows]
