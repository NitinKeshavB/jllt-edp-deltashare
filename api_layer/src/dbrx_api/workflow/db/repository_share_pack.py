"""
Share Pack Repository

Repository for share pack CRUD operations with SCD Type 2 tracking.
"""

from typing import List, Dict, Any, Optional
from uuid import UUID
import asyncpg
from loguru import logger

from dbrx_api.workflow.db.repository_base import BaseRepository
from dbrx_api.workflow.enums import SharePackStatus, Strategy


class SharePackRepository(BaseRepository):
    """Share pack repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "share_packs", "share_pack_id")

    async def create_from_config(
        self,
        share_pack_id: UUID,
        share_pack_name: str,
        requested_by: str,
        strategy: str,
        config: Dict[str, Any],
        file_format: str,
        original_filename: str,
        tenant_id: Optional[UUID] = None,
        project_id: Optional[UUID] = None,
    ) -> UUID:
        """
        Create a new share pack from uploaded configuration.

        Args:
            share_pack_id: Unique identifier
            share_pack_name: Display name
            requested_by: Email of requestor
            strategy: NEW or UPDATE
            config: SharePackConfig as dict (stored as JSONB)
            file_format: yaml or xlsx
            original_filename: Original uploaded filename
            tenant_id: Resolved tenant ID (optional, resolved during provisioning if None)
            project_id: Resolved project ID (optional, resolved during provisioning if None)

        Returns:
            record_id (UUID) of created version
        """
        import json

        fields = {
            "share_pack_name": share_pack_name,
            "requested_by": requested_by,
            "strategy": strategy,
            "share_pack_status": SharePackStatus.IN_PROGRESS.value,
            "provisioning_status": "Uploaded - queued for validation",
            "error_message": "",
            "config": json.dumps(config),  # JSONB
            "file_format": file_format,
            "original_filename": original_filename,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "is_deleted": False,
        }

        return await self.create_or_update(
            share_pack_id,
            fields,
            created_by=requested_by,
            change_reason="Initial upload",
        )

    async def update_status(
        self,
        share_pack_id: UUID,
        new_status: str,
        provisioning_status: str = "",
        error_message: str = "",
        updated_by: str = "orchestrator",
    ) -> UUID:
        """
        Update share pack status (SCD2).

        Args:
            share_pack_id: Share pack identifier
            new_status: New status (IN_PROGRESS, COMPLETED, FAILED, etc.)
            provisioning_status: Detailed provisioning status message
            error_message: Error message if status is FAILED
            updated_by: Who/what is updating the status

        Returns:
            record_id (UUID) of new version
        """
        current = await self.get_current(share_pack_id)
        if not current:
            raise ValueError(f"SharePack {share_pack_id} not found")

        fields = dict(current)
        # Remove SCD2 columns and entity_id
        for key in ["record_id", "version", "created_by", "change_reason",
                    "effective_from", "effective_to", "is_current", self.entity_id_col]:
            fields.pop(key, None)

        # Update status fields
        fields["share_pack_status"] = new_status
        if provisioning_status:
            fields["provisioning_status"] = provisioning_status
        if error_message:
            fields["error_message"] = error_message

        return await self.create_or_update(
            share_pack_id,
            fields,
            updated_by,
            f"Status changed to {new_status}",
        )

    async def update_tenant_and_project(
        self,
        share_pack_id: UUID,
        tenant_id: UUID,
        project_id: UUID,
        updated_by: str = "orchestrator",
    ) -> UUID:
        """
        Update tenant_id and project_id after resolution.

        Args:
            share_pack_id: Share pack identifier
            tenant_id: Resolved tenant ID
            project_id: Resolved project ID
            updated_by: Who/what is updating

        Returns:
            record_id (UUID) of new version
        """
        current = await self.get_current(share_pack_id)
        if not current:
            raise ValueError(f"SharePack {share_pack_id} not found")

        fields = dict(current)
        for key in ["record_id", "version", "created_by", "change_reason",
                    "effective_from", "effective_to", "is_current", self.entity_id_col]:
            fields.pop(key, None)

        fields["tenant_id"] = tenant_id
        fields["project_id"] = project_id

        return await self.create_or_update(
            share_pack_id,
            fields,
            updated_by,
            "Tenant and project resolved",
        )

    async def list_by_status(
        self,
        status: str,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all share packs with a specific status.

        Args:
            status: Status to filter by (IN_PROGRESS, COMPLETED, FAILED, etc.)
            limit: Optional limit on number of results

        Returns:
            List of share pack dicts
        """
        limit_clause = f"LIMIT {limit}" if limit else ""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE share_pack_status = $1 AND is_current = true AND is_deleted = false
                ORDER BY effective_from DESC
                {limit_clause}
                """,
                status,
            )
            return [dict(row) for row in rows]

    async def list_by_tenant(
        self,
        tenant_id: UUID,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all share packs for a specific tenant.

        Args:
            tenant_id: Tenant identifier
            limit: Optional limit on number of results

        Returns:
            List of share pack dicts
        """
        limit_clause = f"LIMIT {limit}" if limit else ""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE tenant_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY effective_from DESC
                {limit_clause}
                """,
                tenant_id,
            )
            return [dict(row) for row in rows]

    async def list_by_requested_by(
        self,
        requested_by: str,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all share packs requested by a specific user.

        Args:
            requested_by: Requestor email
            limit: Optional limit on number of results

        Returns:
            List of share pack dicts
        """
        limit_clause = f"LIMIT {limit}" if limit else ""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE requested_by = $1 AND is_current = true AND is_deleted = false
                ORDER BY effective_from DESC
                {limit_clause}
                """,
                requested_by,
            )
            return [dict(row) for row in rows]

    async def get_by_name(
        self,
        share_pack_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get share pack by name (latest version).

        Args:
            share_pack_name: Share pack name

        Returns:
            Share pack dict or None if not found
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE share_pack_name = $1 AND is_current = true AND is_deleted = false
                ORDER BY effective_from DESC
                LIMIT 1
                """,
                share_pack_name,
            )
            return dict(row) if row else None
