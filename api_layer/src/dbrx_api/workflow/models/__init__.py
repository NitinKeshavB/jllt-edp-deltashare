"""
Workflow Models Module

All Pydantic models for workflow system:
- Share pack configuration models (from YAML/Excel)
- Database entity models (SCD Type 2)
- Metrics and sync models (append-only)
"""

# Metrics Models (Append-Only)
from dbrx_api.workflow.models.metrics import JobMetrics
from dbrx_api.workflow.models.metrics import Notification
from dbrx_api.workflow.models.metrics import ProjectCost
from dbrx_api.workflow.models.metrics import SyncJob
from dbrx_api.workflow.models.pipeline import Pipeline
from dbrx_api.workflow.models.project import Project
from dbrx_api.workflow.models.recipient import Recipient
from dbrx_api.workflow.models.request import Request
from dbrx_api.workflow.models.share import Share

# Share Pack Configuration Models (from YAML/Excel)
from dbrx_api.workflow.models.share_pack import CronSchedule
from dbrx_api.workflow.models.share_pack import DeltaShareConfig
from dbrx_api.workflow.models.share_pack import PipelineConfig
from dbrx_api.workflow.models.share_pack import RecipientConfig
from dbrx_api.workflow.models.share_pack import ShareConfig
from dbrx_api.workflow.models.share_pack import SharePackConfig
from dbrx_api.workflow.models.share_pack import SharePackMetadata

# Sync Entity Models (SCD Type 2)
from dbrx_api.workflow.models.sync_entities import ADGroup
from dbrx_api.workflow.models.sync_entities import DatabricksObject
from dbrx_api.workflow.models.sync_entities import User

# Database Entity Models (SCD Type 2)
from dbrx_api.workflow.models.tenant import Tenant
from dbrx_api.workflow.models.tenant import TenantRegion

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
