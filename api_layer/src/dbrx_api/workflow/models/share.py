"""
Share Model

Database model for Delta Shares.
"""

from datetime import datetime
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field


class Share(BaseModel):
    """Share database model."""

    record_id: UUID
    share_id: UUID
    share_pack_id: UUID
    share_name: str
    databricks_share_id: str  # Share name from Databricks SDK
    description: Optional[str] = None
    storage_root: Optional[str] = None
    share_assets: List[str] = Field(default_factory=list)  # From JSONB
    recipients_attached: List[str] = Field(default_factory=list)  # From JSONB (recipient names)

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
