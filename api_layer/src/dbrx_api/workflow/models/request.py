"""
Request Model

Database model for share pack requests.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class Request(BaseModel):
    """Request database model."""

    record_id: UUID
    request_id: UUID
    project_id: UUID
    share_pack_id: UUID
    request_description: str
    status: str  # IN_PROGRESS, COMPLETED, FAILED, etc.
    request_type: str  # NEW, UPDATE, DELETE
    approver_status: str  # approved, declined, request_more_info
    assigned_datetime: Optional[datetime] = None
    completed_datetime: Optional[datetime] = None

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
