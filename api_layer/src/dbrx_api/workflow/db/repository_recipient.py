"""
Recipient Repository

Repository for Delta Share recipient CRUD operations with SCD Type 2 tracking.
"""

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


class RecipientRepository(BaseRepository):
    """Recipient repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "recipients", "recipient_id")

    async def create_from_config(
        self,
        recipient_id: UUID,
        share_pack_id: UUID,
        recipient_name: str,
        databricks_recipient_id: str,
        recipient_contact_email: str,
        recipient_type: str,
        recipient_databricks_org: Optional[str] = None,
        ip_access_list: Optional[List[str]] = None,
        token_expiry_days: int = 30,
        token_rotation_enabled: bool = False,
        description: Optional[str] = None,
        created_by: str = "orchestrator",
    ) -> UUID:
        """
        Create a new recipient from provisioning.

        Args:
            recipient_id: Unique identifier
            share_pack_id: Parent share pack ID
            recipient_name: Recipient display name
            databricks_recipient_id: Databricks recipient ID from SDK
            recipient_contact_email: Contact email
            recipient_type: D2D or D2O
            recipient_databricks_org: Databricks org (D2D only)
            ip_access_list: IP allowlist (D2O only)
            token_expiry_days: Token expiry in days
            token_rotation_enabled: Enable token rotation
            description: Recipient description/comment
            created_by: Who is creating

        Returns:
            record_id (UUID) of created version
        """
        import json

        # Check if recipient already exists (from previous share pack or API).
        # Reuse its recipient_id so the SCD2 layer properly expires the old version
        # instead of hitting a unique index violation on recipient_name.
        existing = await self.list_by_recipient_name(recipient_name)
        if existing:
            recipient_id = existing[0]["recipient_id"]

        fields = {
            "share_pack_id": share_pack_id,
            "recipient_name": recipient_name,
            "recipient_databricks_id": databricks_recipient_id,
            "recipient_contact_email": recipient_contact_email,
            "recipient_type": recipient_type,
            "recipient_databricks_org": recipient_databricks_org,
            "client_ip_addresses": json.dumps(_normalize_json_data(ip_access_list or [])),
            "token_expiry_days": token_expiry_days,
            "token_rotation": token_rotation_enabled,
            "description": description or "",
            "is_deleted": False,
            "request_source": "share_pack",
        }

        return await self.create_or_update(recipient_id, fields, created_by, "Provisioned from share pack")

    async def upsert_from_config(
        self,
        share_pack_id: UUID,
        recipient_name: str,
        databricks_recipient_id: str,
        recipient_contact_email: str,
        recipient_type: str,
        recipient_databricks_org: Optional[str] = None,
        ip_access_list: Optional[List[str]] = None,
        token_expiry_days: int = 30,
        token_rotation_enabled: bool = False,
        description: Optional[str] = None,
        created_by: str = "orchestrator",
        recipient_id: Optional[UUID] = None,
    ) -> UUID:
        """
        Create or update a recipient in the data model (SCD2 + audit trail).

        If recipient_id is provided, use it. Otherwise look up by share_pack_id and
        recipient_name; if not found in current share pack, search across ALL share packs
        by recipient_name to avoid unique index violations.
        Logs CREATED or UPDATED via BaseRepository.create_or_update (audit trail).

        Args:
            share_pack_id: Parent share pack ID
            recipient_name: Recipient display name
            databricks_recipient_id: Databricks recipient ID/name from SDK
            recipient_contact_email: Contact email
            recipient_type: D2D or D2O
            recipient_databricks_org: Databricks org (D2D only)
            ip_access_list: IP allowlist (D2O only)
            token_expiry_days: Token expiry in days
            token_rotation_enabled: Enable token rotation
            description: Recipient description/comment
            created_by: Who is creating/updating
            recipient_id: If provided, use this business key; else resolve by share_pack_id + recipient_name

        Returns:
            recipient_id (business key) used for the record
        """
        import json

        if recipient_id is None:
            # First: try current share pack
            existing_list = await self.list_by_share_pack(share_pack_id)
            match = next((r for r in existing_list if r.get("recipient_name") == recipient_name), None)
            if not match:
                # Fallback: search across ALL share packs by name.
                # This handles cross-share-pack updates and avoids unique index violations
                # on (recipient_name) WHERE is_current=true AND is_deleted=false.
                all_records = await self.list_by_recipient_name(recipient_name)
                match = all_records[0] if all_records else None
            recipient_id = match["recipient_id"] if match else uuid4()
            is_update = match is not None
        else:
            is_update = await self.exists(recipient_id)

        change_reason = "Updated from share pack provisioning" if is_update else "Provisioned from share pack"

        fields = {
            "share_pack_id": share_pack_id,
            "recipient_name": recipient_name,
            "recipient_databricks_id": databricks_recipient_id,
            "recipient_contact_email": recipient_contact_email,
            "recipient_type": recipient_type,
            "recipient_databricks_org": recipient_databricks_org,
            "client_ip_addresses": json.dumps(_normalize_json_data(ip_access_list or [])),
            "token_expiry_days": token_expiry_days,
            "token_rotation": token_rotation_enabled,
            "description": description or "",
            "is_deleted": False,
            "request_source": "share_pack",
        }

        await self.create_or_update(recipient_id, fields, created_by, change_reason)
        return recipient_id

    async def create_or_upsert_from_api(
        self,
        recipient_name: str,
        databricks_recipient_id: str,
        recipient_type: str,
        recipient_contact_email: Optional[str] = None,
        recipient_databricks_org: Optional[str] = None,
        ip_access_list: Optional[List[str]] = None,
        description: Optional[str] = None,
        created_by: str = "api",
    ) -> UUID:
        """
        Create or update a recipient record from direct API/APIM (share_pack_id=NULL, request_source=api).

        If a current record exists with this recipient_name, upsert it; otherwise create new.
        Preserves token_expiry_days and token_rotation from existing DB record when updating.
        """
        import json

        existing_list = await self.list_by_recipient_name(recipient_name)
        if existing_list:
            recipient_id = existing_list[0]["recipient_id"]
            change_reason = "Updated via API"
            # Preserve token fields from existing record (not available from Databricks API)
            token_expiry = existing_list[0].get("token_expiry_days", 30) or 30
            token_rotation_val = existing_list[0].get("token_rotation", False)
        else:
            recipient_id = uuid4()
            change_reason = "Created via API"
            token_expiry = 30
            token_rotation_val = False

        fields = {
            "share_pack_id": None,
            "recipient_name": recipient_name,
            "recipient_databricks_id": databricks_recipient_id,
            "recipient_contact_email": recipient_contact_email or "",
            "recipient_type": recipient_type,
            "recipient_databricks_org": recipient_databricks_org or "",
            "client_ip_addresses": json.dumps(_normalize_json_data(ip_access_list or [])),
            "token_expiry_days": token_expiry,
            "token_rotation": token_rotation_val,
            "description": description or "",
            "is_deleted": False,
            "request_source": "api",
        }

        await self.create_or_update(recipient_id, fields, created_by, change_reason)
        return recipient_id

    async def list_by_share_pack(
        self,
        share_pack_id: UUID,
    ) -> List[Dict[str, Any]]:
        """
        Get all recipients for a share pack.

        Args:
            share_pack_id: Share pack ID

        Returns:
            List of recipient dicts
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE share_pack_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY recipient_name
                """,
                share_pack_id,
            )
            return [dict(row) for row in rows]

    async def list_by_recipient_name(
        self,
        recipient_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Get all current recipient records with this name (across all share packs).

        Used when deleting a recipient via API to soft-delete all DB rows for that name.

        Args:
            recipient_name: Recipient name (e.g. from Databricks)

        Returns:
            List of recipient dicts (each has recipient_id, share_pack_id, etc.)
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE recipient_name = $1 AND is_current = true AND is_deleted = false
                ORDER BY share_pack_id
                """,
                recipient_name,
            )
            return [dict(row) for row in rows]
