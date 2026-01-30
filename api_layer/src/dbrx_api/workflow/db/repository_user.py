"""
User Repository

Repository for Azure AD user CRUD operations with SCD Type 2 tracking.
"""

from uuid import UUID
import asyncpg
from dbrx_api.workflow.db.repository_base import BaseRepository


class UserRepository(BaseRepository):
    """User repository (synced from Azure AD)."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "users", "user_id")

    async def get_by_email(self, email: str):
        """Get user by email."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM deltashare.{self.table}
                WHERE email = $1 AND is_current = true AND is_deleted = false
                """,
                email,
            )
            return dict(row) if row else None
