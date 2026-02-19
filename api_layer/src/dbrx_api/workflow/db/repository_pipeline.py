"""
Pipeline Repository

Repository for Databricks pipeline CRUD operations with SCD Type 2 tracking.
"""

import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID
from uuid import uuid4

import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


def _normalize_json_data(data: Any) -> Any:
    """
    Normalize data for consistent JSON serialization.

    - Sorts lists to prevent order-based false positives
    - Sorts dict keys (json.dumps does this with sort_keys=True)
    - Removes duplicates from lists

    Args:
        data: Data to normalize (list, dict, or other)

    Returns:
        Normalized data
    """
    if isinstance(data, list):
        # Sort and deduplicate list (preserve strings, numbers, etc.)
        try:
            # Remove duplicates while preserving order, then sort
            unique_items = list(dict.fromkeys(data))
            return sorted(unique_items)
        except TypeError:
            # If items aren't comparable (mixed types), just deduplicate
            return list(dict.fromkeys(data))
    elif isinstance(data, dict):
        # Recursively normalize nested structures
        return {k: _normalize_json_data(v) for k, v in data.items()}
    else:
        return data


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
        # Check if pipeline already exists (from previous share pack or API).
        # Reuse its pipeline_id so the SCD2 layer properly expires the old version
        # instead of hitting a unique index violation on pipeline_name.
        existing = await self.list_by_pipeline_name(pipeline_name)
        if existing:
            pipeline_id = existing[0]["pipeline_id"]

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
            "cron_timezone": timezone,
            "serverless": serverless,
            "tags": json.dumps(_normalize_json_data(tags or {})),
            "notification_list": json.dumps(_normalize_json_data(notification_emails or [])),
            "is_deleted": False,
            "request_source": "share_pack",
        }

        return await self.create_or_update(pipeline_id, fields, created_by, "Provisioned from share pack")

    async def upsert_from_config(
        self,
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
        tags: Optional[Dict[str, str]] = None,
        notification_emails: Optional[List[str]] = None,
        created_by: str = "orchestrator",
        pipeline_id: Optional[UUID] = None,
    ) -> UUID:
        """Create or update a pipeline in the data model (SCD2 + audit trail)."""
        if pipeline_id is None:
            # First: try current share pack
            existing_list = await self.list_by_share_pack(share_pack_id)
            match = next(
                (r for r in existing_list if r.get("pipeline_name") == pipeline_name),
                None,
            )
            if not match:
                # Fallback: search across ALL share packs by name.
                # This handles cross-share-pack updates and avoids unique index violations
                # on (pipeline_name) WHERE is_current=true AND is_deleted=false.
                all_records = await self.list_by_pipeline_name(pipeline_name)
                match = all_records[0] if all_records else None
            pipeline_id = match["pipeline_id"] if match else uuid4()
            is_update = match is not None
        else:
            is_update = await self.exists(pipeline_id)
        change_reason = "Updated from share pack provisioning" if is_update else "Provisioned from share pack"
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
            "cron_timezone": timezone,
            "serverless": serverless,
            "tags": json.dumps(_normalize_json_data(tags or {})),
            "notification_list": json.dumps(_normalize_json_data(notification_emails or [])),
            "is_deleted": False,
            "request_source": "share_pack",
        }
        return await self.create_or_update(pipeline_id, fields, created_by, change_reason)

    async def list_by_pipeline_name(
        self,
        pipeline_name: str,
    ) -> List[Dict[str, Any]]:
        """Get all current pipeline records with this pipeline_name (any share_pack_id or NULL)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM deltashare.pipelines
                WHERE pipeline_name = $1 AND is_current = true AND is_deleted = false
                ORDER BY share_pack_id NULLS LAST
                """,
                pipeline_name,
            )
            return [dict(row) for row in rows]

    async def create_or_upsert_from_api(
        self,
        pipeline_name: str,
        databricks_pipeline_id: str,
        asset_name: str = "",
        source_table: str = "",
        target_table: str = "",
        schedule_type: str = "CRON",
        cron_expression: str = "",
        timezone: str = "UTC",
        serverless: bool = False,
        key_columns: str = "",
        scd_type: str = "2",
        tags: Optional[Dict[str, str]] = None,
        notification_emails: Optional[List[str]] = None,
        created_by: str = "api",
    ) -> UUID:
        """
        Create or update a pipeline record from direct API/APIM.

        When tags or notification_emails are provided, they replace the stored values.
        This is used after any Databricks pipeline mutation to keep the DB in sync.
        """
        existing_list = await self.list_by_pipeline_name(pipeline_name)
        if existing_list:
            pipeline_id = existing_list[0]["pipeline_id"]
            change_reason = "Updated via API"
            # Preserve share_pack_id and share_id from existing record (not available from Databricks API)
            existing_share_pack_id = existing_list[0].get("share_pack_id")
            existing_share_id = existing_list[0].get("share_id")
            # Preserve schedule fields from existing record when not provided
            existing_schedule_type = existing_list[0].get("schedule_type", "CRON") or "CRON"
            existing_cron = existing_list[0].get("cron_expression", "") or ""
            existing_tz = existing_list[0].get("cron_timezone", "UTC") or "UTC"
        else:
            pipeline_id = uuid4()
            change_reason = "Created via API"
            existing_share_pack_id = None
            existing_share_id = None
            existing_schedule_type = "CRON"
            existing_cron = ""
            existing_tz = "UTC"

        asset = asset_name or pipeline_name
        source = source_table or pipeline_name
        target = target_table or pipeline_name

        fields: Dict[str, Any] = {
            "share_pack_id": existing_share_pack_id,
            "share_id": existing_share_id,
            "pipeline_name": pipeline_name,
            "databricks_pipeline_id": databricks_pipeline_id,
            "asset_name": asset,
            "source_table": source,
            "target_table": target,
            "scd_type": scd_type,
            "key_columns": key_columns,
            "schedule_type": schedule_type or existing_schedule_type,
            "cron_expression": cron_expression or existing_cron,
            "cron_timezone": timezone or existing_tz,
            "serverless": serverless,
            "tags": json.dumps(_normalize_json_data(tags or {})),
            "notification_list": json.dumps(_normalize_json_data(notification_emails or [])),
            "is_deleted": False,
            "request_source": "api",
        }
        return await self.create_or_update(pipeline_id, fields, created_by, change_reason)

    async def update_schedule_from_api(
        self,
        pipeline_name: str,
        databricks_job_id: str,
        cron_expression: Optional[str] = None,
        timezone_str: Optional[str] = None,
        created_by: str = "api",
    ) -> None:
        """
        Update schedule fields on an API-created pipeline (share_pack_id IS NULL, request_source=api).
        No-op if no such pipeline exists. Used after schedule create/update via direct API.
        """
        existing_list = await self.list_by_pipeline_name(pipeline_name)
        api_rows = [r for r in existing_list if r.get("request_source") == "api"]
        if not api_rows:
            return
        pipeline_id = api_rows[0][self.entity_id_col]
        current = await self.get_current(pipeline_id)
        if not current:
            return
        scd2_columns = {
            "record_id",
            "effective_from",
            "effective_to",
            "is_current",
            "version",
            "created_by",
            "change_reason",
            self.entity_id_col,
        }
        fields = {k: v for k, v in current.items() if k not in scd2_columns}
        fields["databricks_job_id"] = databricks_job_id
        if cron_expression is not None:
            fields["cron_expression"] = cron_expression
        if timezone_str is not None:
            fields.pop("timezone", None)
            fields["cron_timezone"] = timezone_str
        await self.create_or_update(
            pipeline_id,
            fields,
            created_by,
            "Schedule updated via API",
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

    async def list_by_databricks_pipeline_id(
        self,
        databricks_pipeline_id: str,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get all current pipeline records for a Databricks pipeline ID.

        Used to check if multiple database records reference the same
        Databricks pipeline (shared pipeline scenario).

        Args:
            databricks_pipeline_id: The Databricks pipeline ID
            include_deleted: Include soft-deleted records

        Returns:
            List of pipeline records
        """
        async with self.pool.acquire() as conn:
            deleted_filter = "" if include_deleted else "AND is_deleted = false"
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE databricks_pipeline_id = $1
                  AND is_current = true
                  {deleted_filter}
                ORDER BY pipeline_name
                """,
                databricks_pipeline_id,
            )
            return [dict(row) for row in rows]
