"""Error handling for FastAPI application and Databricks SDK exceptions."""

import pydantic
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from loguru import logger

from dbrx_api.monitoring.logger import log_response_info

# Import Databricks SDK exceptions with fallback for when SDK is not available
try:
    from databricks.sdk.errors import BadRequest
    from databricks.sdk.errors import DatabricksError
    from databricks.sdk.errors import NotFound
    from databricks.sdk.errors import PermissionDenied
    from databricks.sdk.errors import Unauthenticated

    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    DatabricksError = Exception  # type: ignore[misc, assignment]
    Unauthenticated = Exception  # type: ignore[misc, assignment]
    PermissionDenied = Exception  # type: ignore[misc, assignment]
    NotFound = Exception  # type: ignore[misc, assignment]
    BadRequest = Exception  # type: ignore[misc, assignment]

# Explicit exports
__all__ = [
    "DATABRICKS_SDK_AVAILABLE",
    "DatabricksError",
    "handle_broad_exceptions",
    "handle_databricks_errors",
    "handle_databricks_connection_error",
    "handle_pydantic_validation_errors",
]


# fastapi docs on middlewares: https://fastapi.tiangolo.com/tutorial/middleware/
async def handle_broad_exceptions(request: Request, call_next):
    """Handle any exception that goes unhandled by a more specific exception handler."""
    try:
        return await call_next(request)
    except Exception as err:  # pylint: disable=broad-except
        error_response = {"detail": "Internal server error", "error_type": type(err).__name__}

        # Get request body from request state (set by RequestContextMiddleware)
        request_body = getattr(request.state, "request_body", None)

        # Log with full context for database storage
        logger.error(
            f"Unhandled exception: {type(err).__name__}: {str(err)}",
            http_status=500,
            status_code=500,  # Also include status_code for consistency
            http_method=request.method,
            url_path=str(request.url.path),
            error_type=type(err).__name__,
            error_message=str(err),
            request_body=request_body,
            response_body=error_response,
            exc_info=True,  # Include full traceback
        )

        response = JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response,
        )
        log_response_info(response)
        return response


# fastapi docs on error handlers: https://fastapi.tiangolo.com/tutorial/handling-errors/
async def handle_pydantic_validation_errors(request: Request, exc: pydantic.ValidationError) -> JSONResponse:
    """Handle Pydantic validation errors."""
    errors = exc.errors()
    error_response = {
        "detail": [
            {
                "msg": error["msg"],
                "input": error["input"],
            }
            for error in errors
        ]
    }

    # Get request body from request state (set by RequestContextMiddleware)
    request_body = getattr(request.state, "request_body", None)

    # Log validation errors with full context
    logger.warning(
        f"Validation error: {len(errors)} validation errors",
        http_status=422,
        status_code=422,  # Also include status_code for consistency
        http_method=request.method,
        url_path=str(request.url.path),
        error_type="ValidationError",
        validation_errors=errors,
        request_body=request_body,
        response_body=error_response,
    )

    response = JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=error_response,
    )
    log_response_info(response)

    return response


async def handle_databricks_errors(request: Request, exc: DatabricksError) -> JSONResponse:
    """
    Handle Databricks SDK errors and convert them to appropriate HTTP responses.

    Maps Databricks-specific exceptions to HTTP status codes:
    - Unauthenticated -> 401 Unauthorized
    - PermissionDenied -> 403 Forbidden
    - NotFound -> 404 Not Found
    - BadRequest -> 400 Bad Request
    - Other DatabricksError -> 502 Bad Gateway (upstream service error)

    Parameters
    ----------
    request : Request
        FastAPI request object
    exc : DatabricksError
        Databricks SDK exception

    Returns
    -------
    JSONResponse
        HTTP response with appropriate status code and error details
    """
    error_message = str(exc)
    error_type = type(exc).__name__

    # Determine HTTP status code based on exception type
    if DATABRICKS_SDK_AVAILABLE:
        if isinstance(exc, Unauthenticated):
            http_status = 401
            error_response = {
                "detail": "Databricks authentication failed. Please verify your credentials.",
                "error_type": error_type,
            }
        elif isinstance(exc, PermissionDenied):
            http_status = 403
            error_response = {
                "detail": "Access denied to the requested Databricks resource.",
                "error_type": error_type,
            }
        elif isinstance(exc, NotFound):
            http_status = 404
            error_response = {
                "detail": "The requested Databricks resource was not found.",
                "error_type": error_type,
            }
        elif isinstance(exc, BadRequest):
            http_status = 400
            error_response = {
                "detail": f"Invalid request to Databricks: {error_message}",
                "error_type": error_type,
            }
        else:
            # Generic DatabricksError - treat as upstream service failure
            http_status = 502
            error_response = {
                "detail": f"Databricks service error: {error_message}",
                "error_type": error_type,
            }
    else:
        # SDK not available, generic error handling
        http_status = 500
        error_response = {
            "detail": "Databricks SDK error occurred",
            "error_type": error_type,
        }

    # Get request body from request state (set by RequestContextMiddleware)
    request_body = getattr(request.state, "request_body", None)

    # Log with full context for database storage
    logger.error(
        f"Databricks API error: {error_type}: {error_message}",
        http_status=http_status,
        status_code=http_status,  # Also include status_code for consistency
        http_method=request.method,
        url_path=str(request.url.path),
        error_type=error_type,
        error_message=error_message,
        databricks_error_details=error_message,
        request_body=request_body,
        response_body=error_response,
        exc_info=True,  # Include full traceback
    )

    response = JSONResponse(
        status_code=http_status,
        content=error_response,
    )

    log_response_info(response)
    return response


def handle_databricks_connection_error(error: Exception, request: Request = None) -> JSONResponse:
    """
    Handle connection errors when communicating with Databricks workspace.

    This function handles network-level errors that occur when trying to
    connect to a Databricks workspace (timeouts, DNS failures, etc.).

    Parameters
    ----------
    error : Exception
        The connection error exception
    request : Request, optional
        FastAPI request object for context

    Returns
    -------
    JSONResponse
        503 Service Unavailable response
    """
    error_message = str(error)
    error_type = type(error).__name__

    # Check for common connection error patterns
    if "timeout" in error_message.lower():
        detail = "Connection to Databricks workspace timed out. Please try again later."
    elif "name or service not known" in error_message.lower() or "nodename nor servname" in error_message.lower():
        detail = "Unable to resolve Databricks workspace URL. Please verify the URL is correct."
    elif "connection refused" in error_message.lower():
        detail = "Connection to Databricks workspace was refused. Please verify the URL is correct."
    elif "ssl" in error_message.lower() or "certificate" in error_message.lower():
        detail = "SSL/TLS error connecting to Databricks workspace."
    else:
        detail = f"Unable to connect to Databricks workspace: {error_message}"

    error_response = {
        "detail": detail,
        "error_type": "ConnectionError",
    }

    # Get request body from request state (set by RequestContextMiddleware) if request is available
    request_body = getattr(request.state, "request_body", None) if request else None

    # Log with full context for database storage
    logger.error(
        f"Databricks connection error: {error_type}: {error_message}",
        http_status=503,
        status_code=503,  # Also include status_code for consistency
        http_method=request.method if request else None,
        url_path=str(request.url.path) if request else None,
        error_type=error_type,
        error_message=error_message,
        connection_error_detail=detail,
        request_body=request_body,
        response_body=error_response,
        exc_info=True,  # Include full traceback
    )

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=error_response,
    )
