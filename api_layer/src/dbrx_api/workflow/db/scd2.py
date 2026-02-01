"""
SCD Type 2 Helper Functions

Generic functions for Slowly Changing Dimension Type 2 operations.
All mutable entities use this pattern: expire current row, insert new version.
Never UPDATE in place - always INSERT new version with incremented version number.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

import asyncpg
from loguru import logger


async def expire_and_insert_scd2(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    new_fields: Dict[str, Any],
    created_by: str,
    change_reason: str,
) -> UUID:
    """
    Generic SCD2 expire-and-insert operation.

    Expires the current version of an entity (sets effective_to=NOW, is_current=false)
    and inserts a new version with incremented version number.

    Args:
        conn: Database connection (must be in a transaction)
        table: Table name (e.g., "tenants", "share_packs")
        entity_id_column: Business key column name (e.g., "tenant_id", "share_pack_id")
        entity_id: Business key value
        new_fields: Dict of fields to set in new version (excluding SCD2 columns)
        created_by: Who/what is creating this version
        change_reason: Why this version is being created

    Returns:
        record_id (UUID) of the newly inserted version

    Raises:
        Exception: If database operations fail
    """
    # 1. Expire current row (if exists)
    old_row = await conn.fetchrow(
        f"""
        UPDATE deltashare.{table}
        SET effective_to = NOW(), is_current = false
        WHERE {entity_id_column} = $1 AND is_current = true
        RETURNING record_id, version
        """,
        entity_id,
    )

    # 2. Determine new version number
    new_version = (old_row["version"] + 1) if old_row else 1

    # 3. Build INSERT statement dynamically
    # Include entity_id column + all provided fields + SCD2 columns
    columns = (
        [entity_id_column]
        + list(new_fields.keys())
        + [
            "version",
            "created_by",
            "change_reason",
            "effective_from",
            "effective_to",
            "is_current",
        ]
    )

    placeholders = [f"${i+1}" for i in range(len(columns))]

    values = (
        [entity_id]
        + list(new_fields.values())
        + [
            new_version,
            created_by,
            change_reason,
        ]
    )
    # Note: effective_from, effective_to, is_current use database defaults via keyword

    insert_sql = f"""
    INSERT INTO deltashare.{table} ({', '.join(columns)})
    VALUES ({', '.join(placeholders)}, NOW(), 'infinity', true)
    RETURNING record_id
    """

    record_id = await conn.fetchval(insert_sql, *values)

    logger.debug(f"SCD2 insert: {table}.{entity_id_column}={entity_id}, version={new_version}, record_id={record_id}")

    return record_id


async def get_current_version(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    include_deleted: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Fetch current version of an entity.

    Args:
        conn: Database connection
        table: Table name
        entity_id_column: Business key column name
        entity_id: Business key value
        include_deleted: If False, exclude soft-deleted rows (default: False)

    Returns:
        Dict of row data or None if not found
    """
    deleted_filter = "" if include_deleted else "AND is_deleted = false"

    row = await conn.fetchrow(
        f"""
        SELECT * FROM deltashare.{table}
        WHERE {entity_id_column} = $1 AND is_current = true {deleted_filter}
        """,
        entity_id,
    )

    if row:
        return dict(row)
    return None


async def get_all_current_versions(
    conn: asyncpg.Connection,
    table: str,
    include_deleted: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch all current versions from a table.

    Args:
        conn: Database connection
        table: Table name
        include_deleted: If False, exclude soft-deleted rows (default: False)

    Returns:
        List of dicts, one per current row
    """
    deleted_filter = "" if include_deleted else "AND is_deleted = false"

    rows = await conn.fetch(
        f"""
        SELECT * FROM deltashare.{table}
        WHERE is_current = true {deleted_filter}
        ORDER BY effective_from DESC
        """
    )

    return [dict(row) for row in rows]


async def get_history(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
) -> List[Dict[str, Any]]:
    """
    Fetch full version history of an entity.

    Args:
        conn: Database connection
        table: Table name
        entity_id_column: Business key column name
        entity_id: Business key value

    Returns:
        List of dicts, one per version, ordered by version number
    """
    rows = await conn.fetch(
        f"""
        SELECT * FROM deltashare.{table}
        WHERE {entity_id_column} = $1
        ORDER BY version ASC
        """,
        entity_id,
    )

    return [dict(row) for row in rows]


async def soft_delete_scd2(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    deleted_by: str,
    deletion_reason: str,
) -> Optional[UUID]:
    """
    Soft delete an entity (SCD2 style).

    Creates a new version with is_deleted=true instead of physically deleting the row.

    Args:
        conn: Database connection (must be in a transaction)
        table: Table name
        entity_id_column: Business key column name
        entity_id: Business key value
        deleted_by: Who/what is deleting this entity
        deletion_reason: Why this entity is being deleted

    Returns:
        record_id (UUID) of the deleted version, or None if entity not found

    Raises:
        Exception: If database operations fail
    """
    # Get current version
    current = await get_current_version(conn, table, entity_id_column, entity_id)
    if not current:
        logger.warning(f"Cannot delete {table}.{entity_id_column}={entity_id} - not found")
        return None

    # Prepare fields for new version (same as current, but is_deleted=true)
    new_fields = {
        k: v
        for k, v in current.items()
        if k
        not in [
            "record_id",
            "version",
            "created_by",
            "change_reason",
            "effective_from",
            "effective_to",
            "is_current",
            entity_id_column,
        ]
    }
    new_fields["is_deleted"] = True

    # Insert new version
    record_id = await expire_and_insert_scd2(
        conn, table, entity_id_column, entity_id, new_fields, deleted_by, deletion_reason
    )

    logger.info(f"Soft deleted {table}.{entity_id_column}={entity_id}, record_id={record_id}")

    return record_id


async def get_point_in_time_version(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    timestamp: Any,  # datetime or str (ISO format)
) -> Optional[Dict[str, Any]]:
    """
    Fetch entity version at a specific point in time.

    Args:
        conn: Database connection
        table: Table name
        entity_id_column: Business key column name
        entity_id: Business key value
        timestamp: Timestamp to query (datetime or ISO string)

    Returns:
        Dict of row data or None if not found
    """
    row = await conn.fetchrow(
        f"""
        SELECT * FROM deltashare.{table}
        WHERE {entity_id_column} = $1
          AND effective_from <= $2
          AND effective_to > $2
        """,
        entity_id,
        timestamp,
    )

    if row:
        return dict(row)
    return None


async def restore_deleted_entity(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    restored_by: str,
    restoration_reason: str,
) -> Optional[UUID]:
    """
    Restore a soft-deleted entity (SCD2 style).

    Creates a new version with is_deleted=false.

    Args:
        conn: Database connection (must be in a transaction)
        table: Table name
        entity_id_column: Business key column name
        entity_id: Business key value
        restored_by: Who/what is restoring this entity
        restoration_reason: Why this entity is being restored

    Returns:
        record_id (UUID) of the restored version, or None if entity not found
    """
    # Get current version (including deleted)
    current = await get_current_version(conn, table, entity_id_column, entity_id, include_deleted=True)
    if not current:
        logger.warning(f"Cannot restore {table}.{entity_id_column}={entity_id} - not found")
        return None

    if not current.get("is_deleted"):
        logger.warning(f"Entity {table}.{entity_id_column}={entity_id} is not deleted - skipping restore")
        return None

    # Prepare fields for new version (same as current, but is_deleted=false)
    new_fields = {
        k: v
        for k, v in current.items()
        if k
        not in [
            "record_id",
            "version",
            "created_by",
            "change_reason",
            "effective_from",
            "effective_to",
            "is_current",
            entity_id_column,
        ]
    }
    new_fields["is_deleted"] = False

    # Insert new version
    record_id = await expire_and_insert_scd2(
        conn, table, entity_id_column, entity_id, new_fields, restored_by, restoration_reason
    )

    logger.info(f"Restored {table}.{entity_id_column}={entity_id}, record_id={record_id}")

    return record_id
