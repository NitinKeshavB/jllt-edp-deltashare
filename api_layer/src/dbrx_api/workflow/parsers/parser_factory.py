"""
Parser Factory

Auto-detects file format and dispatches to appropriate parser.
"""

from typing import Union, BinaryIO
from loguru import logger

from dbrx_api.workflow.models.share_pack import SharePackConfig


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
        return parse_yaml(file_content)

    elif ext in ("xlsx", "xls"):
        from dbrx_api.workflow.parsers.excel_parser import parse_excel
        return parse_excel(file_content)

    else:
        raise ValueError(
            f"Unsupported file format: .{ext} (supported: .yaml, .yml, .xlsx, .xls)"
        )


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
            warnings.append(
                f"D2D recipient '{recipient.name}' missing recipient_databricks_org"
            )

    # Check for D2O recipients without IP list (warning only)
    for recipient in config.recipient:
        if recipient.type == "D2O" and not recipient.recipient_ips:
            warnings.append(
                f"D2O recipient '{recipient.name}' has no IP access list (will allow all IPs)"
            )

    return warnings
