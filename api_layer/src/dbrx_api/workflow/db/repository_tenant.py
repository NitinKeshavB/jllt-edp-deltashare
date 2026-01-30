"""
Tenant Repository

Repository for tenant (business line) CRUD operations with SCD Type 2 tracking.
"""

from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


class TenantRepository(BaseRepository):
    """Tenant repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "tenants", "tenant_id")

    async def create_tenant(
        self,
        tenant_id: UUID,
        business_line_name: str,
        short_name: Optional[str] = None,
        executive_team: Optional[List[str]] = None,
        configurator_ad_group: Optional[List[str]] = None,
        owner: Optional[str] = None,
        contact_email: Optional[str] = None,
        created_by: str = "workflow_system",
    ) -> UUID:
        """
        Create a new tenant.

        Args:
            tenant_id: Unique tenant identifier
            business_line_name: Business line name
            short_name: Short name/abbreviation
            executive_team: List of executive emails/groups
            configurator_ad_group: List of configurator emails/groups
            owner: Owner email
            contact_email: Contact email
            created_by: Who is creating this tenant

        Returns:
            record_id (UUID) of created version
        """
        import json

        fields = {
            "business_line_name": business_line_name,
            "short_name": short_name,
            "executive_team": json.dumps(executive_team or []),
            "configurator_ad_group": json.dumps(configurator_ad_group or []),
            "owner": owner,
            "contact_email": contact_email,
            "is_deleted": False,
        }

        return await self.create_or_update(
            tenant_id, fields, created_by, "Initial creation"
        )

    async def get_by_name(
        self,
        business_line_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get tenant by business line name.

        Args:
            business_line_name: Business line name

        Returns:
            Tenant dict or None if not found
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE business_line_name = $1 AND is_current = true AND is_deleted = false
                """,
                business_line_name,
            )
            return dict(row) if row else None

    async def get_or_create_by_name(
        self,
        business_line_name: str,
        created_by: str = "workflow_system",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Get tenant by name, or create if doesn't exist.

        Args:
            business_line_name: Business line name
            created_by: Who is creating (if needed)
            **kwargs: Additional fields for creation (short_name, executive_team, etc.)

        Returns:
            Tenant dict
        """
        tenant = await self.get_by_name(business_line_name)
        if tenant:
            return tenant

        # Create new tenant
        tenant_id = uuid4()
        await self.create_tenant(
            tenant_id,
            business_line_name,
            created_by=created_by,
            **kwargs,
        )

        # Return newly created tenant
        return await self.get_current(tenant_id)


class TenantRegionRepository(BaseRepository):
    """Tenant region repository (workspace URL mappings)."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "tenant_regions", "tenant_region_id")

    async def create_tenant_region(
        self,
        tenant_region_id: UUID,
        tenant_id: UUID,
        region: str,
        workspace_url: str,
        created_by: str = "workflow_system",
    ) -> UUID:
        """
        Create a new tenant region mapping.

        Args:
            tenant_region_id: Unique identifier
            tenant_id: Parent tenant ID
            region: Region code (AM, EMEA)
            workspace_url: Databricks workspace URL
            created_by: Who is creating this mapping

        Returns:
            record_id (UUID) of created version
        """
        fields = {
            "tenant_id": tenant_id,
            "region": region.upper(),
            "workspace_url": workspace_url,
            "is_deleted": False,
        }

        return await self.create_or_update(
            tenant_region_id, fields, created_by, "Initial creation"
        )

    async def get_by_tenant_and_region(
        self,
        tenant_id: UUID,
        region: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get tenant region by tenant ID and region.

        Args:
            tenant_id: Tenant ID
            region: Region code (AM, EMEA)

        Returns:
            Tenant region dict or None if not found
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE tenant_id = $1 AND region = $2 AND is_current = true AND is_deleted = false
                """,
                tenant_id,
                region.upper(),
            )
            return dict(row) if row else None

    async def list_by_tenant(
        self,
        tenant_id: UUID,
    ) -> List[Dict[str, Any]]:
        """
        Get all regions for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            List of tenant region dicts
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE tenant_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY region
                """,
                tenant_id,
            )
            return [dict(row) for row in rows]
