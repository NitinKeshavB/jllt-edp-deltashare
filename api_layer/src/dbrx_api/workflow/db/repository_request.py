"""
Request Repository

Repository for request CRUD operations with SCD Type 2 tracking.
"""

from uuid import UUID
import asyncpg
from dbrx_api.workflow.db.repository_base import BaseRepository


class RequestRepository(BaseRepository):
    """Request repository with domain-specific queries."""

    def __init__(self, pool: asyncpg.Pool):
        super().__init__(pool, "requests", "request_id")
