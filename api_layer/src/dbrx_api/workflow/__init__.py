"""
Delta Share Workflow Module

This module provides workflow automation for Delta Share provisioning including:
- Share pack upload and validation (YAML/Excel)
- Async provisioning via Azure Storage Queue
- SCD Type 2 historical tracking for all entities
- Azure AD and Databricks metadata sync
- Job metrics and cost collection
"""

__version__ = "1.0.0"
