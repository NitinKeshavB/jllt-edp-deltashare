"""
Workflow API Response Schemas

Response models for workflow API endpoints (PascalCase fields per existing pattern).
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from uuid import UUID
from datetime import datetime


# ════════════════════════════════════════════════════════════════════════════
# Share Pack Schemas
# ════════════════════════════════════════════════════════════════════════════


class SharePackUploadResponse(BaseModel):
    """Response after share pack upload."""

    Message: str
    SharePackId: str
    SharePackName: str
    Status: str  # IN_PROGRESS, VALIDATION_FAILED, etc.
    ValidationErrors: List[str] = Field(default_factory=list)
    ValidationWarnings: List[str] = Field(default_factory=list)


class SharePackStatusResponse(BaseModel):
    """Share pack status details."""

    SharePackId: str
    SharePackName: str
    Status: str  # IN_PROGRESS, COMPLETED, FAILED
    Strategy: str  # NEW, UPDATE
    ProvisioningStatus: str
    ErrorMessage: str
    RequestedBy: str
    CreatedAt: datetime
    LastUpdated: datetime


class SharePackHistoryItem(BaseModel):
    """Single version from share pack history."""

    Version: int
    Status: str
    EffectiveFrom: datetime
    EffectiveTo: datetime
    ChangedBy: str
    ChangeReason: str


class SharePackHistoryResponse(BaseModel):
    """Share pack version history."""

    Message: str
    SharePackId: str
    SharePackName: str
    History: List[SharePackHistoryItem]


class SharePackListResponse(BaseModel):
    """List of share packs."""

    Message: str
    Count: int
    SharePacks: List[SharePackStatusResponse]


# ════════════════════════════════════════════════════════════════════════════
# Tenant Schemas
# ════════════════════════════════════════════════════════════════════════════


class TenantResponse(BaseModel):
    """Tenant details."""

    TenantId: str
    BusinessLineName: str
    ShortName: Optional[str]
    ExecutiveTeam: List[str]
    ConfiguratorAdGroup: List[str]
    Owner: Optional[str]
    ContactEmail: Optional[str]


class TenantListResponse(BaseModel):
    """List of tenants."""

    Message: str
    Count: int
    Tenants: List[TenantResponse]


# ════════════════════════════════════════════════════════════════════════════
# Health Check Schema
# ════════════════════════════════════════════════════════════════════════════


class WorkflowHealthResponse(BaseModel):
    """Workflow system health check."""

    Message: str
    DatabaseConnected: bool
    QueueConnected: bool
    TablesCount: int
