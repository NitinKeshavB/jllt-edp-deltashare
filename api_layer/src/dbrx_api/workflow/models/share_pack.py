"""
Share Pack Configuration Models

Pydantic models matching the canonical YAML structure from WORKFLOW_MVP_PLAN.md Section 1.
These models are used for both YAML and Excel parsing.
"""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

# ════════════════════════════════════════════════════════════════════════════
# Metadata Section
# ════════════════════════════════════════════════════════════════════════════


class SharePackMetadata(BaseModel):
    """Share pack metadata (tenant, project, request context)."""

    version: str = "1.0"
    last_updated: Optional[str] = None
    owner: Optional[str] = None
    contact_email: Optional[str] = None
    business_line: str  # Tenant name
    delta_share_region: str  # AM or EMEA
    configurator: str  # AD group or comma-separated emails
    approver: str  # AD group or comma-separated emails
    executive_team: str  # AD group or comma-separated emails
    approver_status: str = "approved"  # approved | declined | request_more_info
    requestor: str  # Email of person submitting
    strategy: str = "NEW"  # NEW or UPDATE
    workspace_url: str  # Databricks workspace URL for provisioning (required)

    @field_validator("workspace_url")
    @classmethod
    def validate_workspace_url(cls, v: str) -> str:
        """Validate workspace URL is HTTPS."""
        if not v.startswith("https://"):
            raise ValueError("workspace_url must be a valid HTTPS URL")
        return v.strip().rstrip("/")

    @field_validator("delta_share_region")
    @classmethod
    def validate_region(cls, v: str) -> str:
        """Validate region is AM or EMEA."""
        v_upper = v.upper()
        if v_upper not in ("AM", "EMEA"):
            raise ValueError("delta_share_region must be AM or EMEA")
        return v_upper

    @field_validator("strategy")
    @classmethod
    def validate_strategy(cls, v: str) -> str:
        """Validate strategy is NEW or UPDATE."""
        v_upper = v.upper()
        if v_upper not in ("NEW", "UPDATE"):
            raise ValueError("strategy must be NEW or UPDATE")
        return v_upper

    @field_validator("approver_status")
    @classmethod
    def validate_approver_status(cls, v: str) -> str:
        """Validate approver status."""
        v_lower = v.lower()
        if v_lower not in ("approved", "declined", "request_more_info", "pending"):
            raise ValueError("approver_status must be approved, declined, request_more_info, or pending")
        return v_lower

    @field_validator("requestor", "contact_email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Basic email validation."""
        if v and "@" not in v:
            raise ValueError(f"Invalid email format: {v}")
        return v


# ════════════════════════════════════════════════════════════════════════════
# Recipient Section
# ════════════════════════════════════════════════════════════════════════════


class RecipientConfig(BaseModel):
    """Recipient configuration (D2D or D2O)."""

    name: str  # Unique recipient name
    type: str  # D2D or D2O
    recipient: str  # Contact email
    recipient_databricks_org: str = ""  # Databricks org/metastore ID (required for D2D)
    recipient_ips: List[str] = Field(default_factory=list)  # IP allowlist (D2O only)
    token_expiry: int = 30  # Days
    token_rotation: bool = False

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate recipient type is D2D or D2O."""
        v_upper = v.upper()
        if v_upper not in ("D2D", "D2O"):
            raise ValueError("type must be D2D or D2O")
        return v_upper

    @field_validator("recipient")
    @classmethod
    def validate_recipient_email(cls, v: str) -> str:
        """Validate recipient email."""
        if "@" not in v:
            raise ValueError(f"Invalid recipient email: {v}")
        return v

    @field_validator("token_expiry")
    @classmethod
    def validate_token_expiry(cls, v: int) -> int:
        """Validate token expiry is positive."""
        if v <= 0:
            raise ValueError("token_expiry must be positive")
        return v

    @model_validator(mode="after")
    def validate_d2d_requirements(self):
        """D2D recipients must have recipient_databricks_org, D2O must not."""
        if self.type == "D2D":
            if not self.recipient_databricks_org:
                raise ValueError(f"D2D recipient '{self.name}' requires recipient_databricks_org")
            if self.recipient_ips:
                raise ValueError(f"D2D recipient '{self.name}' cannot have recipient_ips")
        elif self.type == "D2O":
            if self.recipient_databricks_org:
                raise ValueError(f"D2O recipient '{self.name}' cannot have recipient_databricks_org")
        return self


# ════════════════════════════════════════════════════════════════════════════
# Share Section
# ════════════════════════════════════════════════════════════════════════════


class DeltaShareConfig(BaseModel):
    """Target workspace configuration for pipelines."""

    ext_catalog_name: str  # Target catalog name
    ext_schema_name: str  # Target schema name
    tags: List[str] = Field(default_factory=list)  # Tags for target tables

    @field_validator("ext_catalog_name", "ext_schema_name")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Validate catalog and schema names are not empty."""
        if not v or not v.strip():
            raise ValueError("Catalog and schema names cannot be empty")
        return v.strip()


class CronSchedule(BaseModel):
    """Cron-based schedule configuration."""

    cron: str
    timezone: str = "UTC"

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        """Basic cron expression validation (5 or 6 fields)."""
        parts = v.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(f"Invalid cron expression: {v} (expected 5 or 6 fields)")
        return v.strip()


class PipelineConfig(BaseModel):
    """Pipeline configuration for a share."""

    name_prefix: str  # Pipeline name = {prefix}_{asset_name}
    source_asset: Optional[str] = None  # Which share_asset this pipeline processes (catalog.schema.table) - OPTIONAL for v1.0 compatibility
    target_asset: Optional[str] = None  # Target table name for the pipeline (catalog.schema.table) - used as pipelines.target_table
    schedule: Union[CronSchedule, str, Dict[str, Any]]  # Cron schedule, "continuous", or old v1.0 dict format
    notification: List[str] = Field(default_factory=list)  # Email/AD group list
    tags: Dict[str, str] = Field(default_factory=dict)  # Key-value tags
    serverless: bool = False  # Use serverless compute
    scd_type: str = "2"  # "1", "2", or "full_refresh"
    key_columns: str = ""  # Comma-separated (required for SCD2)
    ext_catalog_name: Optional[str] = None  # Override target catalog (falls back to delta_share config)
    ext_schema_name: Optional[str] = None  # Override target schema (falls back to delta_share config)

    @field_validator("name_prefix")
    @classmethod
    def validate_name_prefix(cls, v: str) -> str:
        """Validate name prefix is not empty."""
        if not v or not v.strip():
            raise ValueError("name_prefix cannot be empty")
        return v.strip()

    @field_validator("scd_type")
    @classmethod
    def validate_scd_type(cls, v: str) -> str:
        """Validate SCD type is valid."""
        if v not in ("1", "2", "full_refresh"):
            raise ValueError("scd_type must be '1', '2', or 'full_refresh'")
        return v

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Union[CronSchedule, str, Dict[str, Any]]) -> Union[CronSchedule, str, Dict[str, Any]]:
        """Validate schedule format - supports v1.0 (dict) and v2.0 (CronSchedule/str) formats."""
        if isinstance(v, str):
            # Must be "continuous"
            if v.lower() != "continuous":
                raise ValueError(f"String schedule must be 'continuous', got: {v}")
        elif isinstance(v, dict):
            # v1.0 format: {asset_name: {cron: "...", timezone: "..."}} or {asset_name: "continuous"}
            # OR v2.0 format: {cron: "...", timezone: "..."}
            # Accept both for backwards compatibility
            pass
        elif not isinstance(v, CronSchedule):
            raise ValueError("Schedule must be CronSchedule object, dict, or 'continuous'")
        return v

    @field_validator("source_asset")
    @classmethod
    def validate_source_asset(cls, v: Optional[str]) -> Optional[str]:
        """Validate source_asset is not empty if provided."""
        if v is not None and (not v or not v.strip()):
            raise ValueError("source_asset cannot be empty string")
        return v.strip() if v else None

    @model_validator(mode="after")
    def migrate_v1_to_v2_and_validate(self):
        """
        Backwards compatibility: Extract source_asset from v1.0 schedule format if missing.

        v1.0 format: schedule = {asset_name: {cron: "...", timezone: "..."}}
        v2.0 format: source_asset = "catalog.schema.table", schedule = {cron: "...", timezone: "..."}
        """
        # If source_asset is missing, try to extract from old v1.0 schedule format
        if self.source_asset is None:
            if isinstance(self.schedule, dict):
                # Check if this is v1.0 format: schedule has asset name as key
                schedule_keys = list(self.schedule.keys())

                # v1.0 format: exactly one key that looks like an asset name (contains dots or is a table name)
                # v2.0 format: keys are "cron" and "timezone"
                if len(schedule_keys) == 1 and schedule_keys[0] not in ["cron", "timezone"]:
                    # v1.0 format detected
                    asset_name = schedule_keys[0]
                    schedule_value = self.schedule[asset_name]

                    self.source_asset = asset_name

                    # Migrate schedule to v2.0 format
                    if isinstance(schedule_value, str):
                        # {asset_name: "continuous"}
                        self.schedule = schedule_value
                    elif isinstance(schedule_value, dict):
                        # {asset_name: {cron: "...", timezone: "..."}}
                        self.schedule = schedule_value

                    from loguru import logger
                    logger.warning(
                        f"[MIGRATION] Pipeline '{self.name_prefix}': Migrated v1.0 schedule format. "
                        f"Extracted source_asset='{self.source_asset}' from schedule. "
                        f"Please update to v2.0 format (explicit source_asset field)."
                    )
                elif "cron" in schedule_keys or "timezone" in schedule_keys:
                    # v2.0 format but source_asset is missing
                    raise ValueError(
                        f"Pipeline '{self.name_prefix}': v2.0 format detected but source_asset is missing. "
                        f"Please add explicit source_asset field."
                    )

        # Validate that source_asset is now set
        if self.source_asset is None:
            raise ValueError(
                f"Pipeline '{self.name_prefix}': source_asset is required. "
                f"Use v2.0 format with explicit source_asset field."
            )

        # Validate key_columns for SCD2
        if self.scd_type == "2" and not self.key_columns:
            raise ValueError("key_columns required for scd_type='2'")

        return self


class ShareConfig(BaseModel):
    """Share configuration with assets, recipients, and pipelines."""

    name: str  # Share name
    share_assets: List[str]  # List of assets (catalog, catalog.schema, catalog.schema.table, etc.)
    recipients: List[str]  # List of recipient names (references RecipientConfig.name)
    delta_share: DeltaShareConfig  # Target workspace config
    pipelines: List[PipelineConfig] = Field(default_factory=list)  # Pipeline configs

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate share name is not empty."""
        if not v or not v.strip():
            raise ValueError("share name cannot be empty")
        return v.strip()

    @field_validator("share_assets")
    @classmethod
    def validate_share_assets(cls, v: List[str]) -> List[str]:
        """Validate assets list is not empty."""
        if not v:
            raise ValueError("share_assets cannot be empty")
        return v

    @field_validator("recipients")
    @classmethod
    def validate_recipients(cls, v: List[str]) -> List[str]:
        """Validate recipients list is not empty."""
        if not v:
            raise ValueError("recipients list cannot be empty")
        return v


# ════════════════════════════════════════════════════════════════════════════
# Main Share Pack Model
# ════════════════════════════════════════════════════════════════════════════


class SharePackConfig(BaseModel):
    """Complete share pack configuration.

    This is the top-level model that represents the entire YAML/Excel file.
    """

    metadata: SharePackMetadata
    recipient: List[RecipientConfig]
    share: List[ShareConfig]

    @model_validator(mode="after")
    def validate_recipient_references(self):
        """Ensure all recipient names referenced in shares exist."""
        known_recipients = {r.name for r in self.recipient}

        for share in self.share:
            for recipient_name in share.recipients:
                if recipient_name not in known_recipients:
                    raise ValueError(f"Share '{share.name}' references unknown recipient '{recipient_name}'")

        return self

    @model_validator(mode="after")
    def validate_unique_names(self):
        """Ensure recipient and share names are unique."""
        # Check recipient names
        recipient_names = [r.name for r in self.recipient]
        if len(recipient_names) != len(set(recipient_names)):
            duplicates = {name for name in recipient_names if recipient_names.count(name) > 1}
            raise ValueError(f"Duplicate recipient names: {duplicates}")

        # Check share names
        share_names = [s.name for s in self.share]
        if len(share_names) != len(set(share_names)):
            duplicates = {name for name in share_names if share_names.count(name) > 1}
            raise ValueError(f"Duplicate share names: {duplicates}")

        return self

    @field_validator("recipient")
    @classmethod
    def validate_has_recipients(cls, v: List[RecipientConfig]) -> List[RecipientConfig]:
        """Ensure at least one recipient exists."""
        if not v:
            raise ValueError("At least one recipient required")
        return v

    @field_validator("share")
    @classmethod
    def validate_has_shares(cls, v: List[ShareConfig]) -> List[ShareConfig]:
        """Ensure at least one share exists."""
        if not v:
            raise ValueError("At least one share required")
        return v
