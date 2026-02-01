"""
Metrics Repository

Repository for job metrics and project cost operations (append-only tables).
"""

from datetime import date
from datetime import datetime
from typing import Optional
from uuid import UUID
from uuid import uuid4

import asyncpg


class JobMetricsRepository:
    """Job metrics repository (append-only, no SCD2)."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(
        self,
        pipeline_id: UUID,
        share_pack_id: UUID,
        databricks_pipeline_id: str,
        run_id: Optional[str] = None,
        status: str = "RUNNING",
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration_seconds: Optional[float] = None,
        rows_processed: Optional[int] = None,
        bytes_processed: Optional[int] = None,
    ) -> UUID:
        """Create a new job metrics entry."""
        metrics_id = uuid4()

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO deltashare.job_metrics
                    (metrics_id, pipeline_id, share_pack_id, databricks_pipeline_id, run_id,
                     status, started_at, completed_at, duration_seconds, rows_processed,
                     bytes_processed, collected_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                """,
                metrics_id,
                pipeline_id,
                share_pack_id,
                databricks_pipeline_id,
                run_id,
                status,
                started_at,
                completed_at,
                duration_seconds,
                rows_processed,
                bytes_processed,
            )

        return metrics_id


class ProjectCostRepository:
    """Project cost repository (append-only, no SCD2)."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def create(
        self,
        project_id: UUID,
        tenant_id: UUID,
        period_start: date,
        period_end: date,
        period_type: str = "weekly",
        databricks_cost: float = 0.0,
        azure_storage_cost: float = 0.0,
        azure_queue_cost: float = 0.0,
        total_cost: float = 0.0,
        currency: str = "USD",
    ) -> UUID:
        """Create a new project cost entry."""
        cost_id = uuid4()

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO deltashare.project_costs
                    (cost_id, project_id, tenant_id, period_start, period_end, period_type,
                     databricks_cost, azure_storage_cost, azure_queue_cost, total_cost, currency, collected_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                """,
                cost_id,
                project_id,
                tenant_id,
                period_start,
                period_end,
                period_type,
                databricks_cost,
                azure_storage_cost,
                azure_queue_cost,
                total_cost,
                currency,
            )

        return cost_id
