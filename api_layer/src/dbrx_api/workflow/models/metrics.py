"""
Metrics Models

Database models for job metrics and project costs (append-only tables).
"""

from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime, date


class JobMetrics(BaseModel):
    """Job Metrics (pipeline run metrics) database model.

    Append-only table - no SCD2 columns.
    """

    metrics_id: UUID
    pipeline_id: UUID
    share_pack_id: UUID
    databricks_pipeline_id: str
    run_id: Optional[str] = None
    status: str  # QUEUED, RUNNING, COMPLETED, FAILED, CANCELED
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    collected_at: datetime

    class Config:
        from_attributes = True


class ProjectCost(BaseModel):
    """Project Cost (aggregated Azure costs) database model.

    Append-only table - no SCD2 columns.
    """

    cost_id: UUID
    project_id: UUID
    tenant_id: UUID
    period_start: date
    period_end: date
    period_type: str = "weekly"  # weekly or monthly
    databricks_cost: float = 0.0
    azure_storage_cost: float = 0.0
    azure_queue_cost: float = 0.0
    total_cost: float = 0.0
    currency: str = "USD"
    collected_at: datetime

    class Config:
        from_attributes = True


class SyncJob(BaseModel):
    """Sync Job (background sync execution record) database model.

    Append-only table - no SCD2 columns.
    """

    sync_job_id: UUID
    sync_type: str  # AD_USERS, AD_GROUPS, DATABRICKS_OBJECTS, JOB_METRICS, PROJECT_COSTS
    workspace_url: Optional[str] = None  # Null for AD syncs
    status: str  # RUNNING, COMPLETED, FAILED, INTERRUPTED
    started_at: datetime
    completed_at: Optional[datetime] = None
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_failed: int = 0
    error_message: str = ""

    class Config:
        from_attributes = True


class Notification(BaseModel):
    """Notification (email notification record) database model.

    Append-only table - no SCD2 columns.
    """

    notification_id: UUID
    notification_type: str  # PROVISION_SUCCESS, PROVISION_FAILURE, SYNC_FAILURE, etc.
    recipient_email: str
    subject: str
    body: str
    related_entity_type: Optional[str] = None  # share_pack, sync_job, pipeline, etc.
    related_entity_id: Optional[UUID] = None
    status: str  # PENDING, SENT, FAILED
    sent_at: Optional[datetime] = None
    error_message: str = ""
    created_at: datetime

    class Config:
        from_attributes = True
