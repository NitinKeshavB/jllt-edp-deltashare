"""
Share Repository

Repository for Delta Share CRUD operations with SCD Type 2 tracking.
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
        ext_catalog_name: Optional[str] = None,
        ext_schema_name: Optional[str] = None,
        prefix_assetname: Optional[str] = None,
        share_tags: Optional[List[str]] = None,
        created_by: str = "orchestrator",
    ) -> UUID:
        """Create a new share from provisioning."""
        # Check if share already exists (from previous share pack or API).
        # Reuse its share_id so the SCD2 layer properly expires the old version
        # instead of hitting a unique index violation on share_name.
        existing = await self.list_by_share_name(share_name)
        if existing:
            share_id = existing[0]["share_id"]

        fields = {
            "share_pack_id": share_pack_id,
            "share_name": share_name,
            "databricks_share_id": databricks_share_id,
            "description": description or "",
            "share_assets": json.dumps(_normalize_json_data(share_assets or [])),
            "recipients": json.dumps(_normalize_json_data(recipients_attached or [])),
            "ext_catalog_name": ext_catalog_name or "",
            "ext_schema_name": ext_schema_name or "",
            "prefix_assetname": prefix_assetname or "",
            "share_tags": json.dumps(_normalize_json_data(share_tags or [])),
            "is_deleted": False,
            "request_source": "share_pack",
        }
        return await self.create_or_update(share_id, fields, created_by, "Provisioned from share pack")

    async def upsert_from_config(
        self,
        share_pack_id: UUID,
        share_name: str,
        databricks_share_id: str,
        share_assets: List[str],
        recipients_attached: List[str],
        description: str = "",
        ext_catalog_name: Optional[str] = None,
        ext_schema_name: Optional[str] = None,
        prefix_assetname: Optional[str] = None,
        share_tags: Optional[List[str]] = None,
        created_by: str = "orchestrator",
        share_id: Optional[UUID] = None,
    ) -> UUID:
        """
        Create or update a share in the data model (SCD2 + audit trail).

        If share_id is provided, use it. Otherwise look up by share_pack_id and
        share_name; if found update that row, else create a new one.
        Logs CREATED or UPDATED via BaseRepository.create_or_update (audit trail).
        """
        if share_id is None:
            # First: try current share pack
            existing_list = await self.list_by_share_pack(share_pack_id)
            match = next((r for r in existing_list if r.get("share_name") == share_name), None)
            if not match:
                # Fallback: search across ALL share packs by name.
                # This handles cross-share-pack updates and avoids unique index violations
                # on (share_name) WHERE is_current=true AND is_deleted=false.
                all_records = await self.list_by_share_name(share_name)
                match = all_records[0] if all_records else None
            share_id = match["share_id"] if match else uuid4()
            is_update = match is not None
        else:
            is_update = await self.exists(share_id)
        change_reason = "Updated from share pack provisioning" if is_update else "Provisioned from share pack"
        fields = {
            "share_pack_id": share_pack_id,
            "share_name": share_name,
            "databricks_share_id": databricks_share_id,
            "description": description or "",
            "share_assets": json.dumps(_normalize_json_data(share_assets or [])),
            "recipients": json.dumps(_normalize_json_data(recipients_attached or [])),
            "ext_catalog_name": ext_catalog_name or "",
            "ext_schema_name": ext_schema_name or "",
            "prefix_assetname": prefix_assetname or "",
            "share_tags": json.dumps(_normalize_json_data(share_tags or [])),
            "is_deleted": False,
            "request_source": "share_pack",
        }
        return await self.create_or_update(share_id, fields, created_by, change_reason)

    async def list_by_share_name(
        self,
        share_name: str,
    ) -> List[Dict[str, Any]]:
        """Get all current share records with this name (any share_pack_id or NULL)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM deltashare.shares
                WHERE share_name = $1 AND is_current = true AND is_deleted = false
                ORDER BY share_pack_id NULLS LAST
                """,
                share_name,
            )
            return [dict(row) for row in rows]

    async def create_or_upsert_from_api(
        self,
        share_name: str,
        databricks_share_id: str,
        share_assets: Optional[List[str]] = None,
        recipients_attached: Optional[List[str]] = None,
        description: str = "",
        created_by: str = "api",
    ) -> UUID:
        """
        Create or update a share record from direct API/APIM (share_pack_id=NULL, request_source=api).

        When share_assets or recipients_attached are provided, they replace the stored values.
        This is used after any Databricks share mutation to keep the DB in sync.
        """
        existing_list = await self.list_by_share_name(share_name)
        if existing_list:
            share_id = existing_list[0]["share_id"]
            change_reason = "Updated via API"
            # Preserve fields from existing record (not available from Databricks API)
            existing_share_pack_id = existing_list[0].get("share_pack_id")
            existing_ext_catalog = existing_list[0].get("ext_catalog_name", "") or ""
            existing_ext_schema = existing_list[0].get("ext_schema_name", "") or ""
            existing_prefix = existing_list[0].get("prefix_assetname", "") or ""
            existing_tags = existing_list[0].get("share_tags", "[]") or "[]"
        else:
            share_id = uuid4()
            change_reason = "Created via API"
            existing_share_pack_id = None
            existing_ext_catalog = ""
            existing_ext_schema = ""
            existing_prefix = ""
            existing_tags = "[]"

        fields: Dict[str, Any] = {
            "share_pack_id": existing_share_pack_id,
            "share_name": share_name,
            "databricks_share_id": databricks_share_id,
            "share_assets": json.dumps(_normalize_json_data(share_assets or [])),
            "recipients": json.dumps(_normalize_json_data(recipients_attached or [])),
            "description": description or "",
            "ext_catalog_name": existing_ext_catalog,
            "ext_schema_name": existing_ext_schema,
            "prefix_assetname": existing_prefix,
            "share_tags": existing_tags,
            "is_deleted": False,
            "request_source": "api",
        }
        return await self.create_or_update(share_id, fields, created_by, change_reason)

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

    async def list_all(
        self,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get all current shares across all share packs."""
        async with self.pool.acquire() as conn:
            deleted_filter = "" if include_deleted else "AND is_deleted = false"
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE is_current = true {deleted_filter}
                ORDER BY share_name
                """
            )
            return [dict(row) for row in rows]
