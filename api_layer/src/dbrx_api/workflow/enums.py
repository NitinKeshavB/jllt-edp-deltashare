"""
Workflow Enums

All enum types used throughout the workflow system.
Values must match exactly with database constraints.
"""

from enum import Enum

# ════════════════════════════════════════════════════════════════════════════
# Geographic and Infrastructure Enums
# ════════════════════════════════════════════════════════════════════════════


class Region(str, Enum):
    """Geographic regions for Databricks workspaces."""

    AM = "AM"  # Americas
    EMEA = "EMEA"  # Europe, Middle East, Africa


class ObjectType(str, Enum):
    """Databricks object types that can be shared."""

    TABLE = "TABLE"
    VIEW = "VIEW"
    SCHEMA = "SCHEMA"
    CATALOG = "CATALOG"
    NOTEBOOK = "NOTEBOOK"
    VOLUME = "VOLUME"


# ════════════════════════════════════════════════════════════════════════════
# Share Pack and Request Enums
# ════════════════════════════════════════════════════════════════════════════


class Strategy(str, Enum):
    """Share pack provisioning strategy."""

    NEW = "NEW"  # Create all entities from scratch
    UPDATE = "UPDATE"  # Diff existing state and apply changes only


class SharePackStatus(str, Enum):
    """Share pack provisioning status."""

    IN_PROGRESS = "IN_PROGRESS"  # Currently being provisioned
    COMPLETED = "COMPLETED"  # Successfully provisioned
    FAILED = "FAILED"  # Provisioning failed
    VALIDATION_FAILED = "VALIDATION_FAILED"  # Pre-provisioning validation failed


class RequestStatus(str, Enum):
    """Request workflow status."""

    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING_APPROVAL = "PENDING_APPROVAL"


class RequestType(str, Enum):
    """Type of request."""

    NEW = "NEW"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class ApproverStatus(str, Enum):
    """Approval decision status."""

    APPROVED = "approved"
    DECLINED = "declined"
    REQUEST_MORE_INFO = "request_more_info"
    PENDING = "pending"


# ════════════════════════════════════════════════════════════════════════════
# Recipient Enums
# ════════════════════════════════════════════════════════════════════════════


class RecipientType(str, Enum):
    """Delta Share recipient authentication type."""

    D2D = "D2D"  # Databricks-to-Databricks (uses DATABRICKS auth)
    D2O = "D2O"  # Databricks-to-Open (uses TOKEN auth)


# ════════════════════════════════════════════════════════════════════════════
# Pipeline Enums
# ════════════════════════════════════════════════════════════════════════════


class SCDType(str, Enum):
    """Slowly Changing Dimension type for pipeline data."""

    TYPE_1 = "1"  # Overwrite (no history)
    TYPE_2 = "2"  # SCD Type 2 (track changes with effective dates)
    FULL_REFRESH = "full_refresh"  # Drop and reload


class ScheduleType(str, Enum):
    """Pipeline execution schedule type."""

    CRON = "CRON"  # Scheduled via cron expression
    CONTINUOUS = "CONTINUOUS"  # Streaming (always running)


class PipelineStatus(str, Enum):
    """Databricks pipeline execution status."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


# ════════════════════════════════════════════════════════════════════════════
# Cost and Metrics Enums
# ════════════════════════════════════════════════════════════════════════════


class PeriodType(str, Enum):
    """Cost aggregation period."""

    WEEKLY = "weekly"
    MONTHLY = "monthly"
    DAILY = "daily"


# ════════════════════════════════════════════════════════════════════════════
# Sync System Enums
# ════════════════════════════════════════════════════════════════════════════


class SyncType(str, Enum):
    """Background sync job types."""

    AD_USERS = "AD_USERS"  # Sync Azure AD users
    AD_GROUPS = "AD_GROUPS"  # Sync Azure AD groups
    DATABRICKS_OBJECTS = "DATABRICKS_OBJECTS"  # Sync Databricks catalogs/schemas/tables
    JOB_METRICS = "JOB_METRICS"  # Collect pipeline job metrics
    PROJECT_COSTS = "PROJECT_COSTS"  # Collect Azure costs


class SyncStatus(str, Enum):
    """Sync job execution status."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"  # Stopped unexpectedly (worker crash)


# ════════════════════════════════════════════════════════════════════════════
# Notification Enums
# ════════════════════════════════════════════════════════════════════════════


class NotificationType(str, Enum):
    """Notification event types."""

    PROVISION_SUCCESS = "PROVISION_SUCCESS"
    PROVISION_FAILURE = "PROVISION_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    SYNC_FAILURE = "SYNC_FAILURE"
    PIPELINE_FAILURE = "PIPELINE_FAILURE"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"


class NotificationStatus(str, Enum):
    """Notification delivery status."""

    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"


# ════════════════════════════════════════════════════════════════════════════
# Audit Trail Enums
# ════════════════════════════════════════════════════════════════════════════


class AuditAction(str, Enum):
    """Audit trail action types."""

    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"
    STATUS_CHANGED = "STATUS_CHANGED"
    PROVISIONED = "PROVISIONED"
    RECREATED = "RECREATED"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"


# ════════════════════════════════════════════════════════════════════════════
# Entity Type Enums (for audit trail and generic operations)
# ════════════════════════════════════════════════════════════════════════════


class EntityType(str, Enum):
    """Entity types in the system."""

    TENANT = "tenant"
    TENANT_REGION = "tenant_region"
    PROJECT = "project"
    USER = "user"
    AD_GROUP = "ad_group"
    DATABRICKS_OBJECT = "databricks_object"
    SHARE_PACK = "share_pack"
    REQUEST = "request"
    RECIPIENT = "recipient"
    SHARE = "share"
    PIPELINE = "pipeline"
    JOB_METRICS = "job_metrics"
    PROJECT_COST = "project_cost"
    SYNC_JOB = "sync_job"
    NOTIFICATION = "notification"
