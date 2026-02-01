"""
Audit Trail Repository

Repository for audit trail operations (append-only table).
"""

from typing import Any
from typing import Dict
from typing import Optional
from uuid import UUID
from uuid import uuid4

import asyncpg


class AuditTrailRepository:
    """Audit trail repository (append-only, no SCD2)."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(
        self,
        entity_type: str,
        entity_id: UUID,
        action: str,
        performed_by: str,
        old_values: Optional[Dict[str, Any]] = None,
        new_values: Optional[Dict[str, Any]] = None,
    ) -> UUID:
        """Create an audit trail entry."""
        import json

        audit_id = uuid4()

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO deltashare.audit_trail
                    (audit_id, entity_type, entity_id, action, performed_by, old_values, new_values, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                """,
                audit_id,
                entity_type,
                entity_id,
                action,
                performed_by,
                json.dumps(old_values, default=str) if old_values else None,
                json.dumps(new_values, default=str) if new_values else None,
            )

        return audit_id
