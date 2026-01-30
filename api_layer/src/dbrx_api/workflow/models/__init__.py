"""
Workflow Models Module

All Pydantic models for workflow system:
- Share pack configuration models (from YAML/Excel)
- Database entity models (SCD Type 2)
- Metrics and sync models (append-only)
"""

# Share Pack Configuration Models (from YAML/Excel)
from dbrx_api.workflow.models.share_pack import (
    SharePackMetadata,
    RecipientConfig,
    DeltaShareConfig,
    CronSchedule,
    PipelineConfig,
    ShareConfig,
    SharePackConfig,
)

# Database Entity Models (SCD Type 2)
from dbrx_api.workflow.models.tenant import Tenant, TenantRegion
from dbrx_api.workflow.models.project import Project
from dbrx_api.workflow.models.request import Request
from dbrx_api.workflow.models.recipient import Recipient
from dbrx_api.workflow.models.share import Share
from dbrx_api.workflow.models.pipeline import Pipeline

# Sync Entity Models (SCD Type 2)
from dbrx_api.workflow.models.sync_entities import User, ADGroup, DatabricksObject

# Metrics Models (Append-Only)
from dbrx_api.workflow.models.metrics import (
    JobMetrics,
    ProjectCost,
    SyncJob,
    Notification,
)

__all__ = [
    # Config models
    "SharePackMetadata",
    "RecipientConfig",
    "DeltaShareConfig",
    "CronSchedule",
    "PipelineConfig",
    "ShareConfig",
    "SharePackConfig",
    # Entity models
    "Tenant",
    "TenantRegion",
    "Project",
    "Request",
    "Recipient",
    "Share",
    "Pipeline",
    # Sync entities
    "User",
    "ADGroup",
    "DatabricksObject",
    # Metrics
    "JobMetrics",
    "ProjectCost",
    "SyncJob",
    "Notification",
]
