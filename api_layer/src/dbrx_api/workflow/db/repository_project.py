"""
Project Repository

Repository for project CRUD operations with SCD Type 2 tracking.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID
from uuid import uuid4

import asyncpg

from dbrx_api.workflow.db.repository_base import BaseRepository


class ProjectRepository(BaseRepository):
    """Project repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "projects", "project_id")

    async def create_project(
        self,
        project_id: UUID,
        project_name: str,
        tenant_id: UUID,
        approver: Optional[List[str]] = None,
        configurator: Optional[List[str]] = None,
        created_by: str = "workflow_system",
    ) -> UUID:
        """
        Create a new project.

        Args:
            project_id: Unique project identifier
            project_name: Project name
            tenant_id: Parent tenant ID
            approver: List of approver emails/groups
            configurator: List of configurator emails/groups
            created_by: Who is creating this project

        Returns:
            record_id (UUID) of created version
        """
        import json

        fields = {
            "project_name": project_name,
            "tenant_id": tenant_id,
            "approver": json.dumps(approver or []),
            "configurator": json.dumps(configurator or []),
            "is_deleted": False,
        }

        return await self.create_or_update(project_id, fields, created_by, "Initial creation")

    async def get_by_tenant_and_name(
        self,
        tenant_id: UUID,
        project_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get project by tenant ID and project name.

        Args:
            tenant_id: Tenant ID
            project_name: Project name

        Returns:
            Project dict or None if not found
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE tenant_id = $1 AND project_name = $2 AND is_current = true AND is_deleted = false
                """,
                tenant_id,
                project_name,
            )
            return dict(row) if row else None

    async def list_by_tenant(
        self,
        tenant_id: UUID,
    ) -> List[Dict[str, Any]]:
        """
        Get all projects for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            List of project dicts
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE tenant_id = $1 AND is_current = true AND is_deleted = false
                ORDER BY project_name
                """,
                tenant_id,
            )
            return [dict(row) for row in rows]

    async def get_or_create_by_tenant_and_name(
        self,
        tenant_id: UUID,
        project_name: str,
        created_by: str = "workflow_system",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Get project by tenant and name, or create if doesn't exist.

        Args:
            tenant_id: Tenant ID
            project_name: Project name
            created_by: Who is creating (if needed)
            **kwargs: Additional fields for creation (approver, configurator)

        Returns:
            Project dict
        """
        project = await self.get_by_tenant_and_name(tenant_id, project_name)
        if project:
            return project

        # Create new project
        project_id = uuid4()
        await self.create_project(
            project_id,
            project_name,
            tenant_id,
            created_by=created_by,
            **kwargs,
        )

        # Return newly created project
        return await self.get_current(project_id)
