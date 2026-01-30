"""
Notification Repository

Repository for notification operations (append-only table).
"""

from typing import Optional
from uuid import UUID, uuid4
import asyncpg


class NotificationRepository:
    """Notification repository (append-only, no SCD2)."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(
        self,
        notification_type: str,
        recipient_email: str,
        subject: str,
        body: str,
        related_entity_type: Optional[str] = None,
        related_entity_id: Optional[UUID] = None,
    ) -> UUID:
        """Create a new notification (PENDING status)."""
        notification_id = uuid4()

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO deltashare.notifications
                    (notification_id, notification_type, recipient_email, subject, body,
                     related_entity_type, related_entity_id, status, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', NOW())
                """,
                notification_id,
                notification_type,
                recipient_email,
                subject,
                body,
                related_entity_type,
                related_entity_id,
            )

        return notification_id

    async def mark_sent(
        self,
        notification_id: UUID,
    ) -> None:
        """Mark notification as SENT."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE deltashare.notifications
                SET status = 'SENT', sent_at = NOW()
                WHERE notification_id = $1
                """,
                notification_id,
            )

    async def mark_failed(
        self,
        notification_id: UUID,
        error_message: str,
    ) -> None:
        """Mark notification as FAILED."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE deltashare.notifications
                SET status = 'FAILED', error_message = $2
                WHERE notification_id = $1
                """,
                notification_id,
                error_message,
            )
