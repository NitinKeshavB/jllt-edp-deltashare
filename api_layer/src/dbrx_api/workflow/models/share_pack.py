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
    contact_email: str  # Contact email (required)
    business_line: str  # Tenant name
    delta_share_region: str  # AM or EMEA
    configurator: str  # AD group or comma-separated emails
    approver: str  # AD group or comma-separated emails
    executive_team: str  # AD group or comma-separated emails
    approver_status: str = "approved"  # approved | declined | request_more_info | pending
    requestor: str  # Email of person submitting
    strategy: str = (
        "NEW"  # Optional. NEW (default) or UPDATE = provision; DELETE = teardown. Omit for normal provisioning.
    )
    workspace_url: str  # Databricks workspace URL for provisioning (required)
    servicenow_ticket: str = Field(alias="servicenow")  # ServiceNow ticket number or link (required)
    project_name: Optional[str] = None  # Project name (used for tenant/project resolution and stable name)
    description: Optional[str] = None  # Share pack description
    client: Optional[str] = None  # Client name (e.g. customer identifier)

    class Config:
        populate_by_name = True  # Allow both 'servicenow_ticket' and 'servicenow'

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
    def validate_strategy(cls, v: Optional[str]) -> str:
        """Validate strategy is NEW, UPDATE, or DELETE. Default NEW when omitted or empty."""
        if not v or not str(v).strip():
            return "NEW"
        v_upper = str(v).strip().upper()
        if v_upper not in ("NEW", "UPDATE", "DELETE"):
            raise ValueError("strategy must be NEW, UPDATE, or DELETE")
        return v_upper

    @field_validator("approver_status")
    @classmethod
    def validate_approver_status(cls, v: str) -> str:
        """Validate approver status."""
        v_lower = v.lower()
        if v_lower not in ("approved", "declined", "request_more_info", "pending"):
            raise ValueError("approver_status must be approved, declined, request_more_info, or pending")
        return v_lower

    @field_validator("project_name", "description", "client")
    @classmethod
    def validate_optional_string_fields(cls, v: Optional[str], info) -> Optional[str]:
        """Strip and validate length for optional metadata fields; empty string becomes None."""
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        field_name = info.field_name
        max_len = {"project_name": 255, "description": 2000, "client": 255}.get(field_name, 500)
        if len(s) > max_len:
            raise ValueError(f"{field_name} must be at most {max_len} characters (got {len(s)})")
        return s

    @field_validator("requestor", "contact_email", "configurator", "approver", "executive_team")
    @classmethod
    def validate_email_or_ad_group(cls, v: str, info) -> str:
        """
        Comprehensive validation for email addresses or AD group names.

        Accepts:
        - Valid email addresses (user@domain.com)
        - AD group names (may or may not contain @)
        - Comma-separated values of the above
        """
        import re

        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty")

        # Handle comma-separated values
        entries = [entry.strip() for entry in v.split(",")]

        for entry in entries:
            if not entry:
                raise ValueError(f"{info.field_name} contains empty value in comma-separated list")

            # If it contains @, validate as email
            if "@" in entry:
                # Email validation
                try:
                    local, domain = entry.rsplit("@", 1)
                except ValueError:
                    raise ValueError(f"Invalid email format in {info.field_name}: {entry}")

                if not local or not domain:
                    raise ValueError(f"Invalid email format (empty local or domain) in {info.field_name}: {entry}")

                # Check domain has at least one dot
                if "." not in domain:
                    raise ValueError(f"Invalid email domain (missing .) in {info.field_name}: {entry}")

                # Check TLD is at least 2 characters
                tld = domain.rsplit(".", 1)[-1]
                if len(tld) < 2:
                    raise ValueError(f"Invalid email TLD (too short) in {info.field_name}: {entry}")

                # Validate common email provider domains to catch typos
                common_providers = {
                    "gmail": "gmail.com",
                    "yahoo": "yahoo.com",
                    "ymail": "ymail.com",
                    "outlook": "outlook.com",
                    "hotmail": "hotmail.com",
                }

                domain_lower = domain.lower()
                for provider, expected_domain in common_providers.items():
                    if domain_lower.startswith(provider + ".") and domain_lower != expected_domain:
                        raise ValueError(
                            f"Invalid email domain in {info.field_name}: {entry} "
                            f"(did you mean {local}@{expected_domain}?)"
                        )

                # Validate local part (before @) contains only valid characters
                if not re.match(r"^[a-zA-Z0-9._+-]+$", local):
                    raise ValueError(f"Invalid email local part in {info.field_name}: {entry}")

            else:
                # AD group name validation (no @ symbol)
                # AD groups can contain letters, numbers, hyphens, underscores, spaces, dots
                if not re.match(r"^[a-zA-Z0-9._\s-]+$", entry):
                    raise ValueError(
                        f"Invalid AD group name in {info.field_name}: {entry} "
                        f"(must contain only letters, numbers, dots, hyphens, underscores, spaces)"
                    )

                # Must be at least 2 characters
                if len(entry) < 2:
                    raise ValueError(f"AD group name too short in {info.field_name}: {entry}")

        return v

    @field_validator("servicenow_ticket")
    @classmethod
    def validate_servicenow_ticket(cls, v: str) -> str:
        """Validate ServiceNow ticket is provided."""
        if not v or not v.strip():
            raise ValueError("ServiceNow ticket number or link is required")
        return v.strip()


# ════════════════════════════════════════════════════════════════════════════
# Recipient Section
# ════════════════════════════════════════════════════════════════════════════


class RecipientConfig(BaseModel):
    """Recipient configuration (D2D or D2O)."""

    name: str  # Unique recipient name
    type: str  # D2D or D2O
    recipient: Optional[
        str
    ] = None  # Recipient email (optional, for identification only, not used as point of contact)
    description: Optional[str] = Field(
        default="", alias="comment"
    )  # Recipient description/comment (accepts both 'description' and 'comment')
    recipient_databricks_org: str = ""  # Databricks org/metastore ID (required for D2D)

    # IP Management - Two approaches (D2O only):
    # Approach 1: Declarative - specify complete desired state (for UPDATE strategy)
    recipient_ips: List[str] = Field(default_factory=list)  # Complete list of IPs that should exist

    # Approach 2: Explicit - specify incremental changes (for NEW strategy and UPDATE strategy)
    recipient_ips_to_add: List[str] = Field(default_factory=list)  # IPs to add (incremental)
    recipient_ips_to_remove: List[str] = Field(default_factory=list)  # IPs to remove (incremental)

    token_expiry: int = 0  # Days (for D2O recipients) - 0 means use Databricks default (120 days)
    token_rotation: bool = False

    class Config:
        populate_by_name = True  # Allow both 'description' and 'comment' to populate the field

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
        """Validate token expiry is non-negative (0 means use Databricks default of 120 days)."""
        if v < 0:
            raise ValueError("token_expiry must be non-negative (0 = Databricks default, >0 = custom days)")
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

    @model_validator(mode="after")
    def validate_ip_lists_no_overlap(self):
        """Validate that recipient_ips_to_add and recipient_ips_to_remove do not overlap."""
        if not self.recipient_ips_to_add or not self.recipient_ips_to_remove:
            # No overlap possible if either list is empty
            return self

        # Normalize IPs (strip whitespace, lowercase)
        ips_to_add = {ip.strip().lower() for ip in self.recipient_ips_to_add if ip.strip()}
        ips_to_remove = {ip.strip().lower() for ip in self.recipient_ips_to_remove if ip.strip()}

        # Find overlapping IPs
        overlapping_ips = ips_to_add & ips_to_remove

        if overlapping_ips:
            overlapping_list = sorted(overlapping_ips)
            raise ValueError(
                f"Recipient '{self.name}': Same IP address(es) cannot be in both "
                f"'recipient_ips_to_add' and 'recipient_ips_to_remove'.\n"
                f"  Conflicting IPs: {', '.join(overlapping_list)}\n\n"
                f"Please choose one action per IP:\n"
                f"  - To ADD an IP: include it ONLY in 'recipient_ips_to_add'\n"
                f"  - To REMOVE an IP: include it ONLY in 'recipient_ips_to_remove'\n\n"
                f"You cannot add and remove the same IP in a single operation."
            )

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
    source_asset: Optional[
        str
    ] = None  # Which share_asset this pipeline processes (catalog.schema.table) - OPTIONAL for v1.0 compatibility
    target_asset: Optional[
        str
    ] = None  # Target table name for the pipeline (catalog.schema.table) - used as pipelines.target_table
    description: Optional[str] = Field(default="", alias="comment")  # Pipeline/schedule description (accepts both)
    schedule: Union[CronSchedule, str, Dict[str, Any]]  # Cron schedule, "continuous", or old v1.0 dict format
    notification: List[str] = Field(default_factory=list)  # Email/AD group list
    tags: Dict[str, str] = Field(default_factory=dict)  # Key-value tags
    serverless: bool = False  # Use serverless compute
    scd_type: str = "2"  # "1", "2", or "full_refresh"
    key_columns: str = ""  # Comma-separated (required for SCD2)
    ext_catalog_name: Optional[str] = None  # Override target catalog (falls back to delta_share config)
    ext_schema_name: Optional[str] = None  # Override target schema (falls back to delta_share config)

    class Config:
        populate_by_name = True  # Allow both 'description' and 'comment'

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
    def validate_schedule(
        cls, v: Union[CronSchedule, str, Dict[str, Any]]
    ) -> Union[CronSchedule, str, Dict[str, Any]]:
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
    description: Optional[str] = Field(default="", alias="comment")  # Share description/comment (accepts both)

    # Share Asset Management - Two approaches:
    # Approach 1: Declarative - specify complete desired state (for UPDATE strategy)
    share_assets: List[str] = Field(default_factory=list)  # Complete list of assets that should be in share

    # Approach 2: Explicit - specify incremental changes (for NEW strategy and UPDATE strategy)
    share_assets_to_add: List[str] = Field(default_factory=list)  # Assets to add to share
    share_assets_to_remove: List[str] = Field(default_factory=list)  # Assets to remove from share

    # Recipient Management - Two approaches:
    # Approach 1: Declarative - specify complete desired state (for UPDATE strategy)
    recipients: List[str] = Field(default_factory=list)  # Complete list of recipients that should be attached

    # Approach 2: Explicit - specify incremental changes (for NEW strategy and UPDATE strategy)
    recipients_to_add: List[str] = Field(default_factory=list)  # Recipients to add to share
    recipients_to_remove: List[str] = Field(default_factory=list)  # Recipients to remove from share

    delta_share: Optional[
        DeltaShareConfig
    ] = None  # Target workspace config (required when pipelines present; optional if no pipelines)
    pipelines: List[PipelineConfig] = Field(default_factory=list)  # Pipeline configs

    class Config:
        populate_by_name = True  # Allow both 'description' and 'comment'

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate share name is not empty."""
        if not v or not v.strip():
            raise ValueError("share name cannot be empty")
        return v.strip()

    @model_validator(mode="after")
    def validate_recipient_lists_no_overlap(self):
        """
        Validate that recipient lists do not have logical conflicts.

        Checks:
        1. recipients_to_add vs recipients_to_remove (explicit approach conflict)
        2. recipients vs recipients_to_remove (declarative vs explicit conflict)
        """
        # Check 1: Explicit approach conflict (recipients_to_add vs recipients_to_remove)
        if self.recipients_to_add and self.recipients_to_remove:
            # Normalize recipient names (strip whitespace, lowercase for comparison)
            recipients_to_add = {name.strip().lower() for name in self.recipients_to_add if name.strip()}
            recipients_to_remove = {name.strip().lower() for name in self.recipients_to_remove if name.strip()}

            # Find overlapping recipients
            overlapping_recipients = recipients_to_add & recipients_to_remove

            if overlapping_recipients:
                overlapping_list = sorted(overlapping_recipients)
                raise ValueError(
                    f"Share '{self.name}': Same recipient(s) cannot be in both "
                    f"'recipients_to_add' and 'recipients_to_remove'.\n"
                    f"  Conflicting recipients: {', '.join(overlapping_list)}\n\n"
                    f"Please choose one action per recipient:\n"
                    f"  - To ADD a recipient: include it ONLY in 'recipients_to_add'\n"
                    f"  - To REMOVE a recipient: include it ONLY in 'recipients_to_remove'\n\n"
                    f"You cannot add and remove the same recipient in a single operation."
                )

        # Check 2: Declarative vs explicit conflict (recipients vs recipients_to_remove)
        if self.recipients and self.recipients_to_remove:
            # Normalize recipient names
            recipients_declarative = {name.strip().lower() for name in self.recipients if name.strip()}
            recipients_to_remove = {name.strip().lower() for name in self.recipients_to_remove if name.strip()}

            # Find conflicting recipients
            conflicting_recipients = recipients_declarative & recipients_to_remove

            if conflicting_recipients:
                conflicting_list = sorted(conflicting_recipients)
                raise ValueError(
                    f"Share '{self.name}': Same recipient(s) cannot be in both "
                    f"'recipients' (declarative) and 'recipients_to_remove' (explicit).\n"
                    f"  Conflicting recipients: {', '.join(conflicting_list)}\n\n"
                    f"This is a logical conflict:\n"
                    f"  - 'recipients' declares: \"The share SHOULD HAVE these recipients\"\n"
                    f"  - 'recipients_to_remove' says: \"REMOVE these recipients from the share\"\n\n"
                    f"Please choose one approach:\n"
                    f"  - DECLARATIVE: Use 'recipients' to specify the complete desired state (remove conflicting recipient from this list to delete it)\n"
                    f"  - EXPLICIT: Use 'recipients_to_add' and 'recipients_to_remove' for incremental changes (don't use 'recipients' field)"
                )

        return self

    @model_validator(mode="after")
    def validate_share_asset_lists_no_overlap(self):
        """
        Validate that share asset lists do not have logical conflicts.

        Checks:
        1. share_assets_to_add vs share_assets_to_remove (explicit approach conflict)
        2. share_assets vs share_assets_to_remove (declarative vs explicit conflict)
        """
        # Check 1: Explicit approach conflict (share_assets_to_add vs share_assets_to_remove)
        if self.share_assets_to_add and self.share_assets_to_remove:
            # Normalize asset names (strip whitespace, lowercase for comparison)
            assets_to_add = {asset.strip().lower() for asset in self.share_assets_to_add if asset.strip()}
            assets_to_remove = {asset.strip().lower() for asset in self.share_assets_to_remove if asset.strip()}

            # Find overlapping assets
            overlapping_assets = assets_to_add & assets_to_remove

            if overlapping_assets:
                overlapping_list = sorted(overlapping_assets)
                raise ValueError(
                    f"Share '{self.name}': Same asset(s) cannot be in both "
                    f"'share_assets_to_add' and 'share_assets_to_remove'.\n"
                    f"  Conflicting assets: {', '.join(overlapping_list)}\n\n"
                    f"Please choose one action per asset:\n"
                    f"  - To ADD an asset: include it ONLY in 'share_assets_to_add'\n"
                    f"  - To REMOVE an asset: include it ONLY in 'share_assets_to_remove'\n\n"
                    f"You cannot add and remove the same asset in a single operation."
                )

        # Check 2: Declarative vs explicit conflict (share_assets vs share_assets_to_remove)
        if self.share_assets and self.share_assets_to_remove:
            # Normalize asset names
            assets_declarative = {asset.strip().lower() for asset in self.share_assets if asset.strip()}
            assets_to_remove = {asset.strip().lower() for asset in self.share_assets_to_remove if asset.strip()}

            # Find conflicting assets
            conflicting_assets = assets_declarative & assets_to_remove

            if conflicting_assets:
                conflicting_list = sorted(conflicting_assets)
                raise ValueError(
                    f"Share '{self.name}': Same asset(s) cannot be in both "
                    f"'share_assets' (declarative) and 'share_assets_to_remove' (explicit).\n"
                    f"  Conflicting assets: {', '.join(conflicting_list)}\n\n"
                    f"This is a logical conflict:\n"
                    f"  - 'share_assets' declares: \"The share SHOULD HAVE these assets\"\n"
                    f"  - 'share_assets_to_remove' says: \"REMOVE these assets from the share\"\n\n"
                    f"Please choose one approach:\n"
                    f"  - DECLARATIVE: Use 'share_assets' to specify the complete desired state (remove conflicting asset from this list to delete it)\n"
                    f"  - EXPLICIT: Use 'share_assets_to_add' and 'share_assets_to_remove' for incremental changes (don't use 'share_assets' field)"
                )

        return self


# ════════════════════════════════════════════════════════════════════════════
# Main Share Pack Model
# ════════════════════════════════════════════════════════════════════════════


class SharePackConfig(BaseModel):
    """Complete share pack configuration.

    This is the top-level model that represents the entire YAML/Excel file.
    You can pass recipients alone, shares alone, or schedules alone (shares with pipeline schedules).
    """

    metadata: SharePackMetadata
    recipient: List[RecipientConfig] = Field(default_factory=list)
    share: List[ShareConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_has_recipients_or_shares(self):
        """At least one of recipient or share section must be non-empty."""
        if not self.recipient and not self.share:
            raise ValueError(
                "Share pack must contain at least one of: 'recipient' or 'share'. "
                "You can pass recipients alone, shares alone, or schedules alone (shares with pipelines)."
            )
        return self

    @model_validator(mode="after")
    def validate_recipient_references(self):
        """Ensure all recipient names referenced in shares exist in YAML.

        Note: Recipients not in YAML will be checked against Databricks during provisioning.
        """
        return self

    @model_validator(mode="after")
    def validate_unique_names(self):
        """Ensure recipient and share names are unique."""
        if self.recipient:
            recipient_names = [r.name for r in self.recipient]
            if len(recipient_names) != len(set(recipient_names)):
                duplicates = {name for name in recipient_names if recipient_names.count(name) > 1}
                raise ValueError(f"Duplicate recipient names: {duplicates}")
        if self.share:
            share_names = [s.name for s in self.share]
            if len(share_names) != len(set(share_names)):
                duplicates = {name for name in share_names if share_names.count(name) > 1}
                raise ValueError(f"Duplicate share names: {duplicates}")
        return self
