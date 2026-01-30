"""
Pipeline Model

Database model for Databricks Delta Live Tables pipelines.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from uuid import UUID
from datetime import datetime


class Pipeline(BaseModel):
    """Pipeline database model."""

    record_id: UUID
    pipeline_id: UUID
    share_id: UUID
    share_pack_id: UUID
    pipeline_name: str
    databricks_pipeline_id: str  # Pipeline ID from Databricks SDK
    asset_name: str  # Source asset name
    source_table: str  # catalog.schema.table
    target_table: str  # {ext_catalog}.{ext_schema}.{prefix}_{asset}
    scd_type: str = "2"  # "1", "2", or "full_refresh"
    key_columns: Optional[str] = None  # Comma-separated
    schedule_type: str  # CRON or CONTINUOUS
    cron_expression: Optional[str] = None
    timezone: str = "UTC"
    serverless: bool = False
    tags: Dict[str, str] = Field(default_factory=dict)  # From JSONB
    notification_emails: List[str] = Field(default_factory=list)  # From JSONB

    # SCD2 columns
    is_deleted: bool = False
    effective_from: datetime
    effective_to: datetime
    is_current: bool
    version: int
    created_by: str
    change_reason: str = ""

    class Config:
        from_attributes = True
