"""
Project Model

Database model for projects within tenants.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class Project(BaseModel):
    """Project database model."""

    record_id: UUID
    project_id: UUID
    project_name: str
    tenant_id: UUID
    approver: List[str] = Field(default_factory=list)  # From JSONB
    configurator: List[str] = Field(default_factory=list)  # From JSONB

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
