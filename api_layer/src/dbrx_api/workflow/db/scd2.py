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
from typing import Set
from uuid import UUID

import asyncpg
from loguru import logger


def _compare_fields(
    current_row: Optional[Dict[str, Any]],
    new_fields: Dict[str, Any],
    exclude_fields: Optional[Set[str]] = None,
) -> bool:
    """
    Compare current row with new fields to detect if anything changed.

    Args:
        current_row: Current database row (or None if entity doesn't exist)
        new_fields: New field values being proposed
        exclude_fields: Fields to exclude from comparison (e.g., audit fields)

    Returns:
        True if data has changed, False if identical
    """
    if not current_row:
        # No current row exists, so this is a new record (changed)
        return True

    # Default exclusions: SCD2 metadata and audit fields
    if exclude_fields is None:
        exclude_fields = {
            "record_id",
            "effective_from",
            "effective_to",
            "is_current",
            "version",
            "created_by",
            "change_reason",
            "created_at",
            "updated_at",
        }

    # Compare each field in new_fields with current row
    for field_name, new_value in new_fields.items():
        if field_name in exclude_fields:
            continue

        current_value = current_row.get(field_name)

        # Normalize values for comparison
        # Convert None to empty string for text fields, empty list for arrays
        if new_value is None and current_value is None:
            continue

        # Handle JSON fields (convert to comparable format)
        import json

        # Check if either value is JSON (dict, list, or JSON string)
        is_json_field = False
        new_parsed = None
        current_parsed = None

        # Try to detect and parse JSON values
        if isinstance(new_value, (list, dict)):
            # New value is already a dict/list
            new_parsed = new_value
            is_json_field = True
        elif isinstance(new_value, str):
            # New value might be a JSON string - try to parse it
            try:
                new_parsed = json.loads(new_value)
                is_json_field = True
            except (json.JSONDecodeError, TypeError, ValueError):
                # Not JSON, treat as regular string
                pass

        if is_json_field:
            # Parse current value as well
            if isinstance(current_value, (list, dict)):
                current_parsed = current_value
            elif isinstance(current_value, str):
                try:
                    current_parsed = json.loads(current_value)
                except (json.JSONDecodeError, TypeError, ValueError):
                    # Current value is not valid JSON, treat as mismatch
                    logger.debug(f"Field '{field_name}' changed: {current_value} → {new_value}")
                    return True
            elif current_value is None:
                # Compare None with parsed JSON value
                current_parsed = None
            else:
                current_parsed = current_value

            # Compare parsed JSON objects using normalized strings
            new_normalized = json.dumps(new_parsed, sort_keys=True)
            current_normalized = json.dumps(current_parsed, sort_keys=True)

            if new_normalized != current_normalized:
                logger.debug(f"Field '{field_name}' changed: {current_normalized} → {new_normalized}")
                return True
        else:
            # Simple value comparison (non-JSON fields)
            if new_value != current_value:
                logger.debug(f"Field '{field_name}' changed: {current_value} → {new_value}")
                return True

    # No changes detected
    return False


async def expire_and_insert_scd2(
    conn: asyncpg.Connection,
    table: str,
    entity_id_column: str,
    entity_id: UUID,
    new_fields: Dict[str, Any],
    created_by: str,
    change_reason: str,
    skip_if_unchanged: bool = True,
) -> UUID:
    """
    Generic SCD2 expire-and-insert operation with change detection.

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
        skip_if_unchanged: If True, don't create new version if data hasn't changed (default: True)

    Returns:
        record_id (UUID) of the newly inserted version (or existing version if unchanged)

    Raises:
        Exception: If database operations fail
    """
    # 0. Check if data has changed (if skip_if_unchanged=True)
    if skip_if_unchanged:
        current_row = await get_current_version(conn, table, entity_id_column, entity_id, include_deleted=False)
        if current_row and not _compare_fields(current_row, new_fields):
            # No changes detected - return existing record_id without versioning
            logger.debug(
                f"Skipping SCD2 version for {table}.{entity_id_column}={entity_id}: no changes detected "
                f"(current version={current_row.get('version')})"
            )
            return current_row["record_id"]

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
            # effective_from, effective_to, is_current values added to placeholders
        ]
    )

    insert_sql = f"""
    INSERT INTO deltashare.{table} ({', '.join(columns)})
    VALUES ({', '.join(placeholders[:len(values)])}, NOW(), '9999-12-31'::timestamp, true)
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
    request_source: Optional[str] = None,
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
        request_source: Origin of delete request (share_pack, api, sync)

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
    if request_source is not None:
        new_fields["request_source"] = request_source

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
