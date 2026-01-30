"""
Base Repository

Base class providing common SCD Type 2 operations for all repositories.
All concrete repositories inherit from this class and add domain-specific queries.
"""

from typing import Dict, Any, Optional, List
from uuid import UUID
import asyncpg
from loguru import logger

from dbrx_api.workflow.db.scd2 import (
    expire_and_insert_scd2,
    get_current_version,
    get_all_current_versions,
    get_history,
    soft_delete_scd2,
    restore_deleted_entity,
    get_point_in_time_version,
)


class BaseRepository:
    """
    Base repository with common SCD2 operations.

    All concrete repositories (TenantRepository, SharePackRepository, etc.) inherit from this.
    Provides generic CRUD operations using SCD2 pattern.
    """

    def __init__(self, pool: asyncpg.Pool, table_name: str, entity_id_column: str):
        """
        Initialize base repository.

        Args:
            pool: asyncpg connection pool
            table_name: Database table name (without schema prefix)
            entity_id_column: Business key column name (e.g., "tenant_id", "share_pack_id")
        """
        self.pool = pool
        self.table = table_name
        self.entity_id_col = entity_id_column

    async def get_current(
        self,
        entity_id: UUID,
        include_deleted: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Get current version of an entity by its business key.

        Args:
            entity_id: Business key (tenant_id, share_pack_id, etc.)
            include_deleted: If True, return deleted entities (default: False)

        Returns:
            Dict of row data or None if not found
        """
        async with self.pool.acquire() as conn:
            return await get_current_version(
                conn, self.table, self.entity_id_col, entity_id, include_deleted
            )

    async def get_all_current(
        self,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get all current versions from this table.

        Args:
            include_deleted: If True, include deleted entities (default: False)

        Returns:
            List of dicts, one per current row
        """
        async with self.pool.acquire() as conn:
            return await get_all_current_versions(conn, self.table, include_deleted)

    async def get_history(
        self,
        entity_id: UUID,
    ) -> List[Dict[str, Any]]:
        """
        Get full version history of an entity.

        Args:
            entity_id: Business key

        Returns:
            List of dicts, one per version, ordered by version number
        """
        async with self.pool.acquire() as conn:
            return await get_history(conn, self.table, self.entity_id_col, entity_id)

    async def get_point_in_time(
        self,
        entity_id: UUID,
        timestamp: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        Get entity version at a specific point in time.

        Args:
            entity_id: Business key
            timestamp: Timestamp to query (datetime or ISO string)

        Returns:
            Dict of row data or None if not found
        """
        async with self.pool.acquire() as conn:
            return await get_point_in_time_version(
                conn, self.table, self.entity_id_col, entity_id, timestamp
            )

    async def create_or_update(
        self,
        entity_id: UUID,
        fields: Dict[str, Any],
        created_by: str,
        change_reason: str = "",
    ) -> UUID:
        """
        Create new or update existing entity (SCD2).

        If entity exists, expires current version and inserts new version.
        If entity doesn't exist, creates first version.

        Args:
            entity_id: Business key
            fields: Dict of fields to set (excluding SCD2 columns and entity_id)
            created_by: Who/what is creating this version
            change_reason: Why this version is being created

        Returns:
            record_id (UUID) of the new version
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                record_id = await expire_and_insert_scd2(
                    conn,
                    self.table,
                    self.entity_id_col,
                    entity_id,
                    fields,
                    created_by,
                    change_reason,
                )

                # Write to audit trail
                await self._write_audit(
                    conn,
                    entity_id,
                    "CREATED" if not change_reason else "UPDATED",
                    created_by,
                    None,
                    fields,
                )

                return record_id

    async def soft_delete(
        self,
        entity_id: UUID,
        deleted_by: str,
        deletion_reason: str,
    ) -> Optional[UUID]:
        """
        Soft delete an entity (sets is_deleted=true via SCD2).

        Args:
            entity_id: Business key
            deleted_by: Who/what is deleting this entity
            deletion_reason: Why this entity is being deleted

        Returns:
            record_id (UUID) of deleted version, or None if not found
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Get current version before deletion
                current = await get_current_version(conn, self.table, self.entity_id_col, entity_id)

                record_id = await soft_delete_scd2(
                    conn,
                    self.table,
                    self.entity_id_col,
                    entity_id,
                    deleted_by,
                    deletion_reason,
                )

                if record_id:
                    # Write to audit trail
                    await self._write_audit(
                        conn,
                        entity_id,
                        "DELETED",
                        deleted_by,
                        current,
                        {"is_deleted": True},
                    )

                return record_id

    async def restore(
        self,
        entity_id: UUID,
        restored_by: str,
        restoration_reason: str,
    ) -> Optional[UUID]:
        """
        Restore a soft-deleted entity (sets is_deleted=false via SCD2).

        Args:
            entity_id: Business key
            restored_by: Who/what is restoring this entity
            restoration_reason: Why this entity is being restored

        Returns:
            record_id (UUID) of restored version, or None if not found
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                record_id = await restore_deleted_entity(
                    conn,
                    self.table,
                    self.entity_id_col,
                    entity_id,
                    restored_by,
                    restoration_reason,
                )

                if record_id:
                    # Write to audit trail
                    await self._write_audit(
                        conn,
                        entity_id,
                        "RECREATED",
                        restored_by,
                        {"is_deleted": True},
                        {"is_deleted": False},
                    )

                return record_id

    async def _write_audit(
        self,
        conn: asyncpg.Connection,
        entity_id: UUID,
        action: str,
        performed_by: str,
        old_values: Optional[Dict[str, Any]],
        new_values: Optional[Dict[str, Any]],
    ) -> None:
        """
        Write an audit trail entry.

        Args:
            conn: Database connection (must be in transaction with main operation)
            entity_id: Business key of entity being modified
            action: Action type (CREATED, UPDATED, DELETED, etc.)
            performed_by: Who/what performed the action
            old_values: Previous values (for updates/deletes)
            new_values: New values (for creates/updates)
        """
        import json

        try:
            await conn.execute(
                """
                INSERT INTO deltashare.audit_trail
                    (entity_type, entity_id, action, performed_by, old_values, new_values)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                self.table,
                entity_id,
                action,
                performed_by,
                json.dumps(old_values, default=str) if old_values else None,
                json.dumps(new_values, default=str) if new_values else None,
            )
        except Exception as e:
            # Audit trail failures should not break the main operation
            logger.error(f"Failed to write audit trail: {e}", exc_info=True)

    async def exists(self, entity_id: UUID, include_deleted: bool = False) -> bool:
        """
        Check if an entity exists.

        Args:
            entity_id: Business key
            include_deleted: If True, include deleted entities (default: False)

        Returns:
            True if entity exists, False otherwise
        """
        result = await self.get_current(entity_id, include_deleted)
        return result is not None

    async def count(self, include_deleted: bool = False) -> int:
        """
        Count current entities in this table.

        Args:
            include_deleted: If True, include deleted entities (default: False)

        Returns:
            Number of current entities
        """
        deleted_filter = "" if include_deleted else "AND is_deleted = false"

        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM deltashare.{self.table}
                WHERE is_current = true {deleted_filter}
                """
            )
            return count
