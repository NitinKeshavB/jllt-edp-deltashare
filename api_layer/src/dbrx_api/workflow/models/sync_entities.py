"""
Sync Entity Models

Database models for entities synced from external sources (Azure AD, Databricks).
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import UUID
from datetime import datetime


class User(BaseModel):
    """User (synced from Azure AD) database model."""

    record_id: UUID
    user_id: UUID
    email: str
    display_name: Optional[str] = None
    job_title: Optional[str] = None
    department: Optional[str] = None
    is_active: bool = True
    ad_object_id: Optional[str] = None  # Azure AD object ID
    source: str = "azure_ad"

    # SCD2 columns
    is_deleted: bool = False
    effective_from: datetime
    effective_to: datetime
    is_current: bool
    version: int
    created_by: str = "ad_sync"
    change_reason: str = ""

    class Config:
        from_attributes = True


class ADGroup(BaseModel):
    """AD Group (synced from Azure AD) database model."""

    record_id: UUID
    group_id: UUID
    group_name: str
    ad_object_id: Optional[str] = None  # Azure AD object ID
    members: List[str] = Field(default_factory=list)  # From JSONB (email list)

    # SCD2 columns
    is_deleted: bool = False
    effective_from: datetime
    effective_to: datetime
    is_current: bool
    version: int
    created_by: str = "ad_sync"
    change_reason: str = ""

    class Config:
        from_attributes = True


class DatabricksObject(BaseModel):
    """Databricks Object (synced from workspace) database model."""

    record_id: UUID
    object_id: UUID
    workspace_url: str
    full_name: str  # catalog.schema.table or catalog.schema
    object_type: str  # TABLE, VIEW, SCHEMA, CATALOG, NOTEBOOK, VOLUME
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None

    # SCD2 columns
    is_deleted: bool = False
    effective_from: datetime
    effective_to: datetime
    is_current: bool
    version: int
    created_by: str = "databricks_sync"
    change_reason: str = ""

    class Config:
        from_attributes = True
