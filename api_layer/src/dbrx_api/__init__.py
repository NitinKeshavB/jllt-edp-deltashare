"""dbrx_api."""

from .monitoring.logger import configure_logger

# Configure logger with default settings (just console logging)
# Azure/PostgreSQL logging can be enabled by calling configure_logger with appropriate settings
configure_logger()
