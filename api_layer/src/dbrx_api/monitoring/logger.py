import json
import logging
import sys
import traceback
from typing import Optional

import loguru
from fastapi import Request
from fastapi import Response
from loguru import logger

# Import handlers at module level for testing
try:
    from dbrx_api.monitoring.azure_blob_handler import AzureBlobLogHandler
except ImportError:
    AzureBlobLogHandler = None

try:
    from dbrx_api.monitoring.postgresql_handler import PostgreSQLLogHandler
except ImportError:
    PostgreSQLLogHandler = None

try:
    from dbrx_api.monitoring.datadog_handler import DatadogLogHandler

    DATADOG_AVAILABLE = True
except ImportError:
    DatadogLogHandler = None
    DATADOG_AVAILABLE = False

# Global handlers for cleanup
_azure_blob_handler: Optional[any] = None
_postgresql_handler: Optional[any] = None
_datadog_handler: Optional[any] = None


# Loggers configuration runs at the start of the application -- src/files_api/__init__.py
def configure_logger(
    enable_blob_logging: bool = False,
    azure_storage_url: Optional[str] = None,
    azure_storage_sas_url: Optional[str] = None,
    blob_container: str = "logging",
    enable_postgresql_logging: bool = False,
    postgresql_connection_string: Optional[str] = None,
    postgresql_table: str = "application_logs",
    postgresql_min_level: str = "WARNING",
    enable_datadog_logging: bool = False,
    dd_api_key: Optional[str] = None,
    dd_env: Optional[str] = None,
):
    """
    Configure loguru logger with multiple sinks.

    Args:
        enable_blob_logging: Enable Azure Blob Storage logging
        azure_storage_url: Azure Storage Account URL
        azure_storage_sas_url: Azure Storage SAS URL for authentication
        blob_container: Blob container name for logs
        enable_postgresql_logging: Enable PostgreSQL logging
        postgresql_connection_string: PostgreSQL connection string
        postgresql_table: PostgreSQL table name for logs
        postgresql_min_level: Minimum log level for PostgreSQL
        enable_datadog_logging: Enable Datadog logging
        dd_api_key: Datadog API key
        dd_env: Environment name for Datadog
    """
    global _azure_blob_handler, _postgresql_handler, _datadog_handler

    # Suppress verbose Azure SDK logging
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("azure.identity").setLevel(logging.ERROR)
    logging.getLogger("azure.core").setLevel(logging.ERROR)
    logging.getLogger("azure.core.pipeline.policies").setLevel(logging.ERROR)

    logger.remove()  # remove the default logger

    # Add stdout handler (always enabled for console output)
    logger.add(
        sink=sys.stdout,
        diagnose=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <bold><white>{message}</white></bold> | <dim>{extra}</dim> {stacktrace}",
        filter=process_log_record,
    )

    # Add Azure Blob Storage handler if enabled
    if enable_blob_logging and azure_storage_url and azure_storage_sas_url:
        try:
            if AzureBlobLogHandler is None:
                logger.warning("AzureBlobLogHandler not available - blob logging disabled")
            else:
                _azure_blob_handler = AzureBlobLogHandler(
                    storage_account_url=azure_storage_url,
                    container_name=blob_container,
                    sas_url=azure_storage_sas_url,
                )
                logger.add(
                    sink=_azure_blob_handler.sink,
                    format="{message}",
                    level="INFO",
                )
                logger.info(
                    "Azure Blob Storage logging enabled",
                    container=blob_container,
                    storage_account=azure_storage_url,
                )
        except Exception as e:
            logger.warning(f"Failed to initialize Azure Blob Storage logging: {e}")
    elif enable_blob_logging:
        logger.warning(
            "Azure Blob Storage logging disabled: missing storage_url or sas_url",
            storage_url_provided=bool(azure_storage_url),
            sas_url_provided=bool(azure_storage_sas_url),
        )

    # Add PostgreSQL handler if enabled
    if enable_postgresql_logging and postgresql_connection_string:
        try:
            if PostgreSQLLogHandler is None:
                logger.warning("PostgreSQLLogHandler not available - PostgreSQL logging disabled")
            else:
                _postgresql_handler = PostgreSQLLogHandler(
                    connection_string=postgresql_connection_string,
                    table_name=postgresql_table,
                    min_level=postgresql_min_level,
                )

                # NOTE: Pool is initialized lazily on first log write to ensure
                # it's created in FastAPI's event loop, not during app startup.

                logger.add(
                    sink=_postgresql_handler.sink,
                    format="{message}",  # Let the handler format the message
                    level=postgresql_min_level,  # Only log critical messages to database
                )
                logger.info(
                    "PostgreSQL logging enabled (pool will initialize on first log)",
                    table=postgresql_table,
                    min_level=postgresql_min_level,
                )
        except Exception as e:
            logger.warning(f"Failed to initialize PostgreSQL logging: {e}")

    # Add Datadog handler if enabled
    if enable_datadog_logging:
        try:
            if DatadogLogHandler is None:
                logger.warning("DatadogLogHandler not available - Datadog logging disabled")
            else:
                _datadog_handler = DatadogLogHandler(
                    api_key=dd_api_key,
                    env=dd_env,
                )
                logger.add(
                    sink=_datadog_handler.sink,
                    format="{message}",
                    level="INFO",
                )
                logger.info(
                    "Datadog logging enabled",
                    env=dd_env or "unknown",
                )
        except Exception as e:
            # Log error but don't crash application - continue without Datadog logging
            logger.error(f"Failed to initialize Datadog logging: {e}", exc_info=True)


def process_log_record(record: "loguru.Record") -> "loguru.Record":
    r"""
    Inject transformed metadata into each log record before they are passed to the formatter.

    For instance,

    1. Serialize the "extra" field to JSON so that renders nicely in CloudWatch logs.
    2. For error logs, add a traceback with \r instead of \n so that CloudWatch does not
       split the traceback into multiple log events.
    """
    extra = record["extra"]

    # serialize "extra" field to JSON
    if extra:
        record["extra"] = json.dumps(extra, default=str)

    # add stacktrace to log record
    record["stacktrace"] = ""
    if record["exception"]:
        err = record["exception"]
        stacktrace = get_formatted_stacktrace(err, replace_newline_character_with_carriage_return=True)
        record["stacktrace"] = stacktrace

    return record


def get_formatted_stacktrace(loguru_record_exception, replace_newline_character_with_carriage_return: bool) -> str:
    """Get the formatted stacktrace for the current exception."""
    exc_type, exc_value, exc_traceback = loguru_record_exception
    stacktrace_: list[str] = traceback.format_exception(exc_type, exc_value, exc_traceback)
    stacktrace: str = "".join(stacktrace_)
    if replace_newline_character_with_carriage_return:
        stacktrace = stacktrace.replace("\n", "\r")
    return stacktrace


def log_request_info(request: Request):
    """Log the request info."""
    request_info = {
        "method": request.method,
        "path": request.url.path,
        "query_params": dict(request.query_params.items()),
        "path_params": dict(request.path_params.items()),
        "headers": dict(request.headers.items()),  # note: logging headers can leak secrets
        "base_url": str(request.base_url),
        "url": str(request.url),
        "client": str(request.client),
        "server": str(request.scope.get("server", "unknown")),
        "cookies": dict(request.cookies.items()),  # note: logging cookies can leak secrets
    }
    logger.debug("Request received", http_request=request_info)


def log_response_info(response: Response):
    """Log the response info."""
    response_info = {
        "status_code": response.status_code,
        "headers": dict(response.headers.items()),
    }
    logger.debug("Response sent", http_response=response_info)
