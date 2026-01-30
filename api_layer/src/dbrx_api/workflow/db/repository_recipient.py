"""
Recipient Repository

Repository for Delta Share recipient CRUD operations with SCD Type 2 tracking.
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


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
        activation_url: Optional[str] = None,
        bearer_token: Optional[str] = None,
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
            activation_url: Activation URL (D2O only)
            bearer_token: Bearer token (D2O only, encrypted)
            created_by: Who is creating

        Returns:
            record_id (UUID) of created version
        """
        import json

        fields = {
            "share_pack_id": share_pack_id,
            "recipient_name": recipient_name,
            "databricks_recipient_id": databricks_recipient_id,
            "recipient_contact_email": recipient_contact_email,
            "recipient_type": recipient_type,
            "recipient_databricks_org": recipient_databricks_org,
            "ip_access_list": json.dumps(ip_access_list or []),
            "token_expiry_days": token_expiry_days,
            "token_rotation_enabled": token_rotation_enabled,
            "activation_url": activation_url,
            "bearer_token": bearer_token,
            "is_deleted": False,
        }

        return await self.create_or_update(
            recipient_id, fields, created_by, "Provisioned from share pack"
        )

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
