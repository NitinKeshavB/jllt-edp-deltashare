"""
Parser Factory

Auto-detects file format and dispatches to appropriate parser.
"""

from typing import Any
from typing import BinaryIO
from typing import Dict
from typing import List
from typing import Union

from loguru import logger

from dbrx_api.workflow.models.share_pack import SharePackConfig

# Placeholder values used when strategy is DELETE (name-only config)
_DELETE_PLACEHOLDER_EMAIL = "delete-placeholder@internal.local"
_DELETE_PLACEHOLDER_ASSET = "_placeholder"
_DELETE_PLACEHOLDER_CATALOG = "_placeholder"
_DELETE_PLACEHOLDER_SCHEMA = "_placeholder"


def normalize_config_for_delete(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize name-only DELETE config into full SharePackConfig shape.

    Accepts:
    - recipient: list of names (strings) or list of {name}
    - share: list of names (strings) or list of {name, pipelines?: [name_prefix or {name_prefix}]}

    Returns dict suitable for SharePackConfig(**...) with placeholder values
    for required fields.
    """
    metadata = data.get("metadata") or {}
    strategy = str(metadata.get("strategy", "")).upper()
    if strategy != "DELETE":
        return data

    # Normalize recipient to list of full objects
    raw_recipient = data.get("recipient") or []
    recipient_list: List[Dict[str, Any]] = []
    for item in raw_recipient:
        if isinstance(item, str) and item.strip():
            recipient_list.append(
                {
                    "name": item.strip(),
                    "type": "D2O",
                    "recipient": _DELETE_PLACEHOLDER_EMAIL,
                    "recipient_databricks_org": "",
                }
            )
        elif isinstance(item, dict) and item.get("name"):
            recipient_list.append(
                {
                    "name": str(item["name"]).strip(),
                    "type": item.get("type", "D2O"),
                    "recipient": item.get("recipient") or _DELETE_PLACEHOLDER_EMAIL,
                    "recipient_databricks_org": item.get("recipient_databricks_org", ""),
                }
            )

    # Normalize share to list of full objects
    raw_share = data.get("share") or []
    share_list: List[Dict[str, Any]] = []
    for item in raw_share:
        if isinstance(item, str) and item.strip():
            share_list.append(
                {
                    "name": item.strip(),
                    "share_assets": [_DELETE_PLACEHOLDER_ASSET],
                    "recipients": [_DELETE_PLACEHOLDER_ASSET],
                    "delta_share": {
                        "ext_catalog_name": _DELETE_PLACEHOLDER_CATALOG,
                        "ext_schema_name": _DELETE_PLACEHOLDER_SCHEMA,
                        "tags": [],
                    },
                    "pipelines": [],
                }
            )
        elif isinstance(item, dict) and item.get("name"):
            pipelines_raw = item.get("pipelines") or []
            pipelines_list = []
            # Placeholder fields required by PipelineConfig when strategy is DELETE
            _placeholder_schedule = {"cron": "0 0 0 1 1 ?", "timezone": "UTC"}
            _placeholder_source_asset = "_placeholder.placeholder.placeholder"
            for p in pipelines_raw:
                if isinstance(p, str) and p.strip():
                    pipelines_list.append(
                        {
                            "name_prefix": p.strip(),
                            "schedule": _placeholder_schedule,
                            "source_asset": _placeholder_source_asset,
                            "scd_type": "1",
                            "key_columns": "",
                        }
                    )
                elif isinstance(p, dict) and (p.get("name_prefix") or p.get("name")):
                    pipelines_list.append(
                        {
                            "name_prefix": str(p.get("name_prefix") or p.get("name")).strip(),
                            "schedule": p.get("schedule", _placeholder_schedule),
                            "source_asset": p.get("source_asset", _placeholder_source_asset),
                            "scd_type": p.get("scd_type", "1"),
                            "key_columns": p.get("key_columns", ""),
                        }
                    )
            share_list.append(
                {
                    "name": str(item["name"]).strip(),
                    "share_assets": [_DELETE_PLACEHOLDER_ASSET],
                    "recipients": [_DELETE_PLACEHOLDER_ASSET],
                    "delta_share": {
                        "ext_catalog_name": _DELETE_PLACEHOLDER_CATALOG,
                        "ext_schema_name": _DELETE_PLACEHOLDER_SCHEMA,
                        "tags": [],
                    },
                    "pipelines": pipelines_list,
                }
            )

    out = dict(data)
    out["recipient"] = recipient_list
    out["share"] = share_list
    return out


def parse_sharepack_file(
    file_content: Union[str, bytes, BinaryIO],
    filename: str,
) -> SharePackConfig:
    """
    Auto-detect format and parse share pack file.

    Supports:
    - YAML (.yaml, .yml)
    - Excel (.xlsx, .xls)

    Args:
        file_content: File content (str for YAML, bytes/BinaryIO for Excel)
        filename: Original filename for format detection

    Returns:
        SharePackConfig instance

    Raises:
        ValueError: If file format is unsupported
        pydantic.ValidationError: If file structure is invalid
    """
    # Detect format from filename extension
    ext = filename.lower().split(".")[-1]

    logger.debug(f"Parsing share pack file: {filename} (detected format: {ext})")

    if ext in ("yaml", "yml"):
        from dbrx_api.workflow.parsers.yaml_parser import parse_yaml

        config = parse_yaml(file_content)
    elif ext in ("xlsx", "xls"):
        from dbrx_api.workflow.parsers.excel_parser import parse_excel

        config = parse_excel(file_content)
    else:
        raise ValueError(f"Unsupported file format: .{ext} (supported: .yaml, .yml, .xlsx, .xls)")

    # Run same strict validation as provisioning (metadata + sharepack config)
    _validate_parsed_config(config)
    return config


def _validate_parsed_config(config: SharePackConfig) -> None:
    """
    Run provisioning-time validation on parsed config so YAML and Excel fail consistently.

    Raises:
        ValueError: If metadata or sharepack config validation fails
    """
    try:
        config_dict = config.model_dump()
    except AttributeError:
        config_dict = config.dict()

    from dbrx_api.workflow.orchestrator.provisioning import validate_metadata
    from dbrx_api.workflow.orchestrator.provisioning import validate_sharepack_config

    validate_metadata(config_dict["metadata"])
    validate_sharepack_config(config_dict)


def validate_sharepack_config(config: SharePackConfig) -> list[str]:
    """
    Perform additional validation on SharePackConfig beyond Pydantic.

    Args:
        config: SharePackConfig instance

    Returns:
        List of validation warning messages (empty if all OK)
    """
    warnings = []

    # Check for empty share assets
    for share in config.share:
        if not share.share_assets:
            warnings.append(f"Share '{share.name}' has no assets")

    # Check for empty pipelines
    for share in config.share:
        if not share.pipelines:
            warnings.append(f"Share '{share.name}' has no pipelines configured")

    # Check for missing recipient contact emails
    for recipient in config.recipient:
        if not recipient.recipient or "@" not in recipient.recipient:
            warnings.append(f"Recipient '{recipient.name}' has invalid contact email")

    # Check for D2D recipients without org ID
    for recipient in config.recipient:
        if recipient.type == "D2D" and not recipient.recipient_databricks_org:
            warnings.append(f"D2D recipient '{recipient.name}' missing recipient_databricks_org")

    # Check for D2O recipients without IP list (warning only)
    for recipient in config.recipient:
        if recipient.type == "D2O" and not recipient.recipient_ips:
            warnings.append(f"D2O recipient '{recipient.name}' has no IP access list (will allow all IPs)")

    return warnings
