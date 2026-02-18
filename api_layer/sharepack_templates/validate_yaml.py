#!/usr/bin/env python3
"""
SharePack YAML File Validator

This script validates the structure and content of SharePack YAML files.
It checks for required fields, data formats, and common errors.

Usage:
    python validate_yaml.py sample_sharepack.yaml
    python validate_yaml.py path/to/your_sharepack.yaml
"""

import re
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

try:
    import yaml
except ImportError:
    print("❌ Missing required package. Install with:")
    print("   pip install pyyaml")
    sys.exit(1)


class SharePackYAMLValidator:
    """Validates SharePack YAML file structure and content."""

    # Required metadata fields
    REQUIRED_METADATA_FIELDS = {
        "requestor",
        "business_line",
        "strategy",
        "delta_share_region",
        "configurator",
        "approver",
        "executive_team",
        "approver_status",
        "workspace_url",
        "servicenow",
    }

    # Required fields per section
    REQUIRED_RECIPIENT_FIELDS = {"name", "type", "recipient"}
    REQUIRED_SHARE_FIELDS = {"name", "share_assets"}
    REQUIRED_PIPELINE_FIELDS = {"name_prefix", "source_asset", "target_asset", "scd_type"}

    def __init__(self, file_path: str):
        """Initialize validator with YAML file path."""
        self.file_path = Path(file_path)
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.data: Dict = {}

    def validate(self) -> Tuple[bool, List[str], List[str], List[str]]:
        """
        Validate the YAML file.

        Returns:
            Tuple of (is_valid, errors, warnings, info)
        """
        # Check file exists
        if not self.file_path.exists():
            self.errors.append(f"File not found: {self.file_path}")
            return False, self.errors, self.warnings, self.info

        self.info.append(f"📁 Validating: {self.file_path.name}")
        self.info.append("")

        # Load YAML
        try:
            with open(self.file_path, "r") as f:
                self.data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            self.errors.append(f"Failed to parse YAML: {e}")
            return False, self.errors, self.warnings, self.info
        except Exception as e:
            self.errors.append(f"Failed to read file: {e}")
            return False, self.errors, self.warnings, self.info

        if not isinstance(self.data, dict):
            self.errors.append("YAML file must contain a mapping/dictionary at root level")
            return False, self.errors, self.warnings, self.info

        # Validate sections
        self._validate_metadata()
        self._validate_recipients()
        self._validate_shares()

        is_valid = len(self.errors) == 0
        return is_valid, self.errors, self.warnings, self.info

    def _validate_metadata(self) -> None:
        """Validate metadata section."""
        self.info.append("📋 Metadata Validation:")

        if "metadata" not in self.data:
            self.errors.append("Missing 'metadata' section")
            self.info.append("   ❌ metadata section missing")
            self.info.append("")
            return

        metadata = self.data["metadata"]
        if not isinstance(metadata, dict):
            self.errors.append("'metadata' must be a mapping/dictionary")
            return

        # Check required fields
        metadata_fields = set(metadata.keys())
        missing_fields = self.REQUIRED_METADATA_FIELDS - metadata_fields

        if missing_fields:
            self.errors.append(f"Missing required metadata fields: {', '.join(sorted(missing_fields))}")
        else:
            self.info.append("   ✅ All required metadata fields present")

        # Validate field values
        self._validate_metadata_values(metadata)
        self.info.append("")

    def _validate_metadata_values(self, metadata: Dict[str, Any]) -> None:
        """Validate metadata field values."""
        # Strategy
        if "strategy" in metadata:
            strategy = str(metadata["strategy"]).upper()
            if strategy not in ["NEW", "UPDATE"]:
                self.errors.append(f"Invalid strategy: '{strategy}'. Must be 'NEW' or 'UPDATE'")
            else:
                self.info.append(f"   ✅ Strategy: {strategy}")

        # Delta share region
        if "delta_share_region" in metadata:
            region = str(metadata["delta_share_region"]).upper()
            if region not in ["AM", "EMEA"]:
                self.errors.append(f"Invalid delta_share_region: '{region}'. Must be 'AM' or 'EMEA'")
            else:
                self.info.append(f"   ✅ Region: {region}")

        # Approver status
        if "approver_status" in metadata:
            status = str(metadata["approver_status"]).lower()
            valid_statuses = ["approved", "declined", "request_more_info", "pending"]
            if status not in valid_statuses:
                self.errors.append(f"Invalid approver_status: '{status}'. Must be one of: {', '.join(valid_statuses)}")

        # Email validation
        email_fields = ["requestor", "configurator", "approver", "executive_team", "contact_email"]
        for field in email_fields:
            if field in metadata:
                value = str(metadata[field])
                if "@" in value and not self._is_valid_email(value):
                    self.warnings.append(f"Potentially invalid email in {field}: {value}")

        # Workspace URL
        if "workspace_url" in metadata:
            url = str(metadata["workspace_url"])
            if not url.startswith("https://"):
                self.errors.append(f"workspace_url must start with https://. Found: {url}")
            elif "azuredatabricks.net" not in url and "cloud.databricks.com" not in url:
                self.warnings.append(f"Workspace URL doesn't match expected patterns: {url}")
            else:
                self.info.append(f"   ✅ Workspace URL: {url}")

        # ServiceNow
        if "servicenow" in metadata:
            sn = str(metadata["servicenow"])
            if not sn or sn.lower() in ["none", ""]:
                self.errors.append("servicenow field is required")
            else:
                self.info.append(f"   ✅ ServiceNow: {sn}")

    def _validate_recipients(self) -> None:
        """Validate recipients section."""
        self.info.append("👥 Recipients Validation:")

        if "recipient" not in self.data and "recipients" not in self.data:
            self.warnings.append("No 'recipient' or 'recipients' section found")
            self.info.append("   ⚠️  No recipients defined")
            self.info.append("")
            return

        # Support both 'recipient' and 'recipients' keys
        recipients = self.data.get("recipient") or self.data.get("recipients")

        if not isinstance(recipients, list):
            self.errors.append("'recipient/recipients' must be a list")
            return

        if len(recipients) == 0:
            self.warnings.append("Recipients list is empty")
            self.info.append("   ⚠️  No recipients defined")
        else:
            self.info.append(f"   Found {len(recipients)} recipient(s)")

        # Validate each recipient
        for idx, recipient in enumerate(recipients):
            self._validate_recipient(idx, recipient)

        self.info.append("")

    def _validate_recipient(self, idx: int, recipient: Dict) -> None:
        """Validate individual recipient."""
        if not isinstance(recipient, dict):
            self.errors.append(f"Recipient {idx + 1}: Must be a mapping/dictionary")
            return

        # Check required fields
        recipient_fields = set(recipient.keys())
        missing_fields = self.REQUIRED_RECIPIENT_FIELDS - recipient_fields

        if missing_fields:
            self.errors.append(f"Recipient {idx + 1}: Missing required fields: {', '.join(sorted(missing_fields))}")

        # Type validation
        if "type" in recipient:
            rec_type = str(recipient["type"]).upper()
            if rec_type not in ["D2O", "D2D"]:
                self.errors.append(f"Recipient {idx + 1}: Invalid type '{rec_type}'. Must be D2O or D2D")
            else:
                self.info.append(f"   ✅ Recipient '{recipient.get('name', idx+1)}': {rec_type}")

            # D2D requires recipient_databricks_org
            if rec_type == "D2D" and not recipient.get("recipient_databricks_org"):
                self.errors.append(f"Recipient {idx + 1}: D2D recipient requires recipient_databricks_org")

        # Email validation
        if "recipient" in recipient and recipient["recipient"]:
            email = str(recipient["recipient"])
            if "@" in email and not self._is_valid_email(email):
                self.warnings.append(f"Recipient {idx + 1}: Potentially invalid email: {email}")

    def _validate_shares(self) -> None:
        """Validate shares section."""
        self.info.append("📦 Shares Validation:")

        if "share" not in self.data and "shares" not in self.data:
            self.errors.append("Missing 'share' or 'shares' section")
            self.info.append("   ❌ No shares defined")
            self.info.append("")
            return

        # Support both 'share' and 'shares' keys
        shares = self.data.get("share") or self.data.get("shares")

        if not isinstance(shares, list):
            self.errors.append("'share/shares' must be a list")
            return

        if len(shares) == 0:
            self.errors.append("Shares list is empty")
            self.info.append("   ❌ No shares defined")
        else:
            self.info.append(f"   Found {len(shares)} share(s)")

        # Validate each share
        for idx, share in enumerate(shares):
            self._validate_share(idx, share)

        self.info.append("")

    def _validate_share(self, idx: int, share: Dict) -> None:
        """Validate individual share."""
        if not isinstance(share, dict):
            self.errors.append(f"Share {idx + 1}: Must be a mapping/dictionary")
            return

        # Check required fields
        share_fields = set(share.keys())
        missing_fields = self.REQUIRED_SHARE_FIELDS - share_fields

        if missing_fields:
            self.errors.append(f"Share {idx + 1}: Missing required fields: {', '.join(sorted(missing_fields))}")

        share_name = share.get("name", f"Share {idx + 1}")
        self.info.append(f"   📄 {share_name}")

        # Validate share_assets
        if "share_assets" in share:
            assets = share["share_assets"]
            if isinstance(assets, list):
                asset_count = len(assets)
                self.info.append(f"      • {asset_count} asset(s)")

                # Check 3-part names
                for asset in assets:
                    asset_str = str(asset)
                    parts = asset_str.split(".")
                    if len(parts) != 3:
                        self.warnings.append(
                            f"{share_name}: Share asset should be 3-part name (catalog.schema.table): {asset_str}"
                        )
            else:
                self.warnings.append(f"{share_name}: share_assets should be a list")

        # Validate delta_share section
        if "delta_share" in share:
            delta_share = share["delta_share"]
            if isinstance(delta_share, dict):
                if "ext_catalog_name" not in delta_share or "ext_schema_name" not in delta_share:
                    self.errors.append(f"{share_name}: delta_share requires ext_catalog_name and ext_schema_name")
                else:
                    self.info.append(
                        f"      • Target: {delta_share['ext_catalog_name']}.{delta_share['ext_schema_name']}"
                    )

        # Validate pipelines
        if "pipelines" in share:
            pipelines = share["pipelines"]
            if isinstance(pipelines, list):
                self.info.append(f"      • {len(pipelines)} pipeline(s)")
                for pipe_idx, pipeline in enumerate(pipelines):
                    self._validate_pipeline(share_name, pipe_idx, pipeline)
            else:
                self.errors.append(f"{share_name}: pipelines must be a list")

    def _validate_pipeline(self, share_name: str, idx: int, pipeline: Dict) -> None:
        """Validate individual pipeline."""
        if not isinstance(pipeline, dict):
            self.errors.append(f"{share_name} Pipeline {idx + 1}: Must be a mapping/dictionary")
            return

        # Check required fields
        pipeline_fields = set(pipeline.keys())
        missing_fields = self.REQUIRED_PIPELINE_FIELDS - pipeline_fields

        if missing_fields:
            self.errors.append(
                f"{share_name} Pipeline {idx + 1}: Missing required fields: {', '.join(sorted(missing_fields))}"
            )

        pipeline_name = pipeline.get("name_prefix", f"Pipeline {idx + 1}")

        # Source asset validation (3-part name)
        if "source_asset" in pipeline:
            source = str(pipeline["source_asset"])
            parts = source.split(".")
            if len(parts) != 3:
                self.errors.append(
                    f"{share_name}/{pipeline_name}: source_asset must be 3-part name (catalog.schema.table): {source}"
                )

        # SCD type validation
        if "scd_type" in pipeline:
            scd_type = str(pipeline["scd_type"])
            if scd_type not in ["1", "2"]:
                self.errors.append(f"{share_name}/{pipeline_name}: scd_type must be '1' or '2', found: {scd_type}")

        # Serverless validation
        if "serverless" in pipeline:
            serverless = pipeline["serverless"]
            if not isinstance(serverless, bool):
                serverless_str = str(serverless).lower()
                if serverless_str not in ["true", "false"]:
                    self.warnings.append(
                        f"{share_name}/{pipeline_name}: serverless should be boolean, found: {serverless}"
                    )

        # Schedule validation
        if "schedule" in pipeline:
            schedule = pipeline["schedule"]
            if isinstance(schedule, dict):
                self._validate_schedule(share_name, pipeline_name, schedule)

        # Tags format validation (semicolon-separated key:value)
        if "tags" in pipeline:
            tags = pipeline["tags"]
            if isinstance(tags, str):
                if ";" in tags:
                    tag_list = [t.strip() for t in tags.split(";")]
                    for tag in tag_list:
                        if ":" not in tag:
                            self.warnings.append(
                                f"{share_name}/{pipeline_name}: Tag should be key:value format: {tag}"
                            )
            elif isinstance(tags, dict):
                # Dict format is also valid
                pass
            else:
                self.warnings.append(
                    f"{share_name}/{pipeline_name}: tags should be string (key:value;key:value) or dict"
                )

    def _validate_schedule(self, share_name: str, pipeline_name: str, schedule: Dict) -> None:
        """Validate pipeline schedule configuration."""
        if "action" in schedule:
            action = str(schedule["action"]).lower()
            if action == "remove":
                # Remove action should only have 'action' field
                if len(schedule) > 1:
                    extra_fields = set(schedule.keys()) - {"action"}
                    self.warnings.append(
                        f"{share_name}/{pipeline_name}: Schedule with action='remove' should not have other fields: {', '.join(extra_fields)}"
                    )
                self.info.append(f"         • Schedule: REMOVE")
            else:
                self.warnings.append(
                    f"{share_name}/{pipeline_name}: Invalid schedule action: {action}. Use 'remove' or omit for add/update"
                )
        else:
            # Add/Update schedule - requires cron and timezone
            if "cron" not in schedule and "timezone" not in schedule:
                self.warnings.append(
                    f"{share_name}/{pipeline_name}: Schedule should have 'cron' and 'timezone' fields"
                )
            else:
                cron = schedule.get("cron", "")
                timezone = schedule.get("timezone", "")

                # Basic cron format check (6 fields for Quartz)
                if cron and cron.lower() != "continuous":
                    cron_parts = str(cron).split()
                    if len(cron_parts) != 6:
                        self.warnings.append(
                            f"{share_name}/{pipeline_name}: Cron expression should have 6 fields (Quartz format): {cron}"
                        )
                    else:
                        self.info.append(f"         • Schedule: {cron} ({timezone})")

    def _is_valid_email(self, email: str) -> bool:
        """Basic email validation."""
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return re.match(pattern, email) is not None


def print_results(is_valid: bool, errors: List[str], warnings: List[str], info: List[str]) -> None:
    """Print validation results."""
    # Print info
    for line in info:
        print(line)

    # Print warnings
    if warnings:
        print("⚠️  WARNINGS:")
        for warning in warnings:
            print(f"   {warning}")
        print()

    # Print errors
    if errors:
        print("❌ ERRORS:")
        for error in errors:
            print(f"   {error}")
        print()

    # Summary
    print("=" * 70)
    if is_valid:
        if warnings:
            print("✅ VALIDATION PASSED (with warnings)")
        else:
            print("✅ VALIDATION PASSED")
        print("\n📤 YAML file is ready to upload!")
    else:
        print("❌ VALIDATION FAILED")
        print(f"\n🔧 Fix {len(errors)} error(s) before uploading")
    print("=" * 70)


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python validate_yaml.py <yaml_file.yaml>")
        print("\nExample:")
        print("  python validate_yaml.py sample_sharepack.yaml")
        sys.exit(1)

    file_path = sys.argv[1]

    # Validate
    validator = SharePackYAMLValidator(file_path)
    is_valid, errors, warnings, info = validator.validate()

    # Print results
    print_results(is_valid, errors, warnings, info)

    # Exit code
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
