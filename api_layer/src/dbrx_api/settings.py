"""Settings for the files API."""

from typing import Optional

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    """
    Settings for the files API.

    [pydantic.BaseSettings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) is a popular
    framework for organizing, validating, and reading configuration values from a variety of sources
    including environment variables.

    This class automatically reads from:
    1. Environment variables (production - Azure Web App Configuration)
    2. .env file (local development)

    Environment variable names are treated case-insensitively, but the canonical
    names used in this project are lowercase (client_id, client_secret, account_id).
    """

    # Databricks Workspace Configuration (DEPRECATED - use X-Workspace-URL header instead)
    dltshr_workspace_url: Optional[str] = None
    """Databricks workspace URL for reference/logging only. Actual workspace URL comes from X-Workspace-URL header."""

    # Databricks Authentication (Service Principal)
    client_id: str
    """Azure Service Principal Client ID for Databricks authentication (required)."""

    client_secret: str
    """Azure Service Principal Client Secret for Databricks authentication (required)."""

    account_id: str
    """Databricks Account ID for authentication (required)."""

    # Optional: Cached authentication token (managed automatically)
    databricks_token: Optional[str] = None
    """Cached Databricks OAuth access token (optional, auto-generated if not provided)."""

    token_expires_at_utc: Optional[str] = None
    """Expiration time for cached token in ISO format (optional, auto-managed)."""

    # Azure Storage for logs
    azure_storage_account_url: Optional[str] = None
    """Azure Storage Account URL for log storage (e.g., https://<account>.blob.core.windows.net)"""

    azure_storage_connection_string: Optional[str] = None
    """Azure Storage Account connection string (alternative to managed identity).
    If provided, will be used instead of DefaultAzureCredential."""

    azure_storage_sas_url: Optional[str] = None
    """Azure Storage Account SAS URL for blob container access.
    Format: https://<account>.blob.core.windows.net/<container>?<sas-token>
    If provided, will be used instead of connection string or managed identity."""

    azure_storage_logs_container: str = "logging"
    """Azure Blob Storage container name for logs."""

    enable_blob_logging: bool = False
    """Enable logging to Azure Blob Storage."""

    # PostgreSQL for critical logs
    postgresql_connection_string: Optional[str] = None
    """PostgreSQL connection string for critical log storage."""

    enable_postgresql_logging: bool = False
    """Enable logging to PostgreSQL database."""

    postgresql_log_table: str = "application_logs"
    """PostgreSQL table name for logs."""

    postgresql_min_log_level: str = "WARNING"
    """Minimum log level to store in PostgreSQL (WARNING, ERROR, CRITICAL)."""

    # Datadog Configuration
    dd_api_key: Optional[str] = None
    """Datadog API key for log ingestion."""

    enable_datadog_logging: bool = False
    """Enable logging to Datadog (disabled by default)."""

    # Workflow System Configuration
    enable_workflow: bool = False
    """Enable workflow system for share pack provisioning (disabled by default)."""

    domain_db_connection_string: Optional[str] = None
    """PostgreSQL connection string for workflow domain database (separate from logging DB)."""

    # Azure Storage Queue for Workflow
    azure_queue_connection_string: Optional[str] = None
    """Azure Storage Queue connection string for workflow processing."""

    azure_queue_name: str = "sharepack-processing"
    """Azure Storage Queue name for share pack provisioning."""

    sync_queue_name: str = "sync-triggers"
    """Azure Storage Queue name for sync job triggers."""

    # Azure AD Sync (Graph API)
    azure_tenant_id: Optional[str] = None
    """Azure AD tenant ID for Graph API access."""

    graph_client_id: Optional[str] = None
    """Azure AD app client ID for Graph API (may differ from Databricks service principal)."""

    graph_client_secret: Optional[str] = None
    """Azure AD app client secret for Graph API."""

    # Notification Settings (SMTP)
    smtp_host: Optional[str] = None
    """SMTP server hostname for email notifications."""

    smtp_port: int = 587
    """SMTP server port (default: 587 for TLS)."""

    smtp_username: Optional[str] = None
    """SMTP authentication username."""

    smtp_password: Optional[str] = None
    """SMTP authentication password."""

    notification_from_email: str = "deltashare-noreply@jll.com"
    """From email address for notifications."""

    # Azure Cost Management
    azure_subscription_id: Optional[str] = None
    """Azure subscription ID for cost collection."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",  # Load from .env file if it exists (local development)
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra environment variables not defined in the model
        # In production, .env file won't exist and pydantic will read from system environment variables
        validate_default=True,  # Validate default values
    )
