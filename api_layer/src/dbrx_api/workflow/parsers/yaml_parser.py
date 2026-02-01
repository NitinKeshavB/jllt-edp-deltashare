"""
YAML Parser

Parses YAML files into SharePackConfig models.
"""

from pathlib import Path
from typing import Union

import yaml
from loguru import logger

from dbrx_api.workflow.models.share_pack import SharePackConfig


def parse_yaml(file_content: Union[str, bytes, Path]) -> SharePackConfig:
    """
    Parse YAML file content into SharePackConfig.

    Args:
        file_content: YAML content as string, bytes, or Path to file

    Returns:
        SharePackConfig instance

    Raises:
        pydantic.ValidationError: If YAML structure doesn't match SharePackConfig schema
        yaml.YAMLError: If YAML syntax is invalid
    """
    # Convert input to string
    if isinstance(file_content, Path):
        content = file_content.read_text(encoding="utf-8")
    elif isinstance(file_content, bytes):
        content = file_content.decode("utf-8")
    else:
        content = file_content

    # Parse YAML
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error: {e}")
        raise ValueError(f"Invalid YAML syntax: {e}")

    if data is None:
        raise ValueError("Empty YAML file")

    # Validate and convert to SharePackConfig
    try:
        config = SharePackConfig(**data)
        logger.debug(f"Successfully parsed YAML: {len(config.recipient)} recipients, {len(config.share)} shares")
        return config
    except Exception as e:
        logger.error(f"SharePackConfig validation error: {e}")
        raise
