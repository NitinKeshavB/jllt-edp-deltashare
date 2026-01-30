"""
Tenant and Tenant Region Models

Database models for tenants (business lines) and their regional workspaces.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class Tenant(BaseModel):
    """Tenant (Business Line) database model."""

    record_id: UUID
    tenant_id: UUID
    business_line_name: str
    short_name: Optional[str] = None
    executive_team: List[str] = Field(default_factory=list)  # From JSONB
    configurator_ad_group: List[str] = Field(default_factory=list)  # From JSONB
    owner: Optional[str] = None
    contact_email: Optional[str] = None

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


class TenantRegion(BaseModel):
    """Tenant Region (workspace URL mapping) database model."""

    record_id: UUID
    tenant_region_id: UUID
    tenant_id: UUID
    region: str  # AM or EMEA
    workspace_url: str

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
