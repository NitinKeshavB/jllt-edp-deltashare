"""FastAPI dependencies for accessing app state."""

import re
import socket
from typing import List
from typing import Tuple
from urllib.parse import urlparse

import httpx
from fastapi import Header
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from loguru import logger

from dbrx_api.dbrx_auth.token_manager import TokenManager
from dbrx_api.settings import Settings

# Valid Databricks workspace URL patterns for different cloud providers
# Azure format: https://adb-<workspace-id>.<region-id>.azuredatabricks.net
# AWS format: https://<workspace-name>.cloud.databricks.com
# GCP format: https://<workspace-name>.gcp.databricks.com
DATABRICKS_URL_PATTERNS: List[str] = [
    r"^https://[a-zA-Z0-9][a-zA-Z0-9.-]*\.azuredatabricks\.net/?$",  # Azure Databricks
    r"^https://[a-zA-Z0-9][a-zA-Z0-9.-]*\.cloud\.databricks\.com/?$",  # AWS Databricks
    r"^https://[a-zA-Z0-9][a-zA-Z0-9.-]*\.gcp\.databricks\.com/?$",  # GCP Databricks
]

# Timeout for workspace reachability check (in seconds)
WORKSPACE_CHECK_TIMEOUT = 5.0


def get_settings(request: Request) -> Settings:
    """
    Get application settings from request state.

    Parameters
    ----------
    request : Request
        FastAPI request object

    Returns
    -------
    Settings
        Application settings instance
    """
    return request.app.state.settings


def get_token_manager(request: Request) -> TokenManager:
    """
    Get token manager from request state.

    The token manager handles cached authentication tokens for Databricks API.

    Parameters
    ----------
    request : Request
        FastAPI request object

    Returns
    -------
    TokenManager
        Token manager instance with cached tokens
    """
    return request.app.state.token_manager


def is_valid_databricks_url(url: str) -> bool:
    """
    Check if a URL matches valid Databricks workspace patterns.

    Parameters
    ----------
    url : str
        URL to validate

    Returns
    -------
    bool
        True if URL matches a valid Databricks pattern, False otherwise
    """
    return any(re.match(pattern, url) for pattern in DATABRICKS_URL_PATTERNS)


async def check_workspace_reachable(url: str) -> Tuple[bool, str]:
    """
    Check if a Databricks workspace URL is reachable.

    Makes a lightweight HEAD request with a short timeout to verify
    the workspace exists and is accessible.

    Parameters
    ----------
    url : str
        Databricks workspace URL to check

    Returns
    -------
    Tuple[bool, str]
        (is_reachable, error_message)
        - (True, "") if workspace is reachable
        - (False, error_message) if workspace is not reachable
    """
    try:
        # Extract hostname for DNS check first (faster failure)
        parsed = urlparse(url)
        hostname = parsed.netloc

        # Quick DNS resolution check
        try:
            socket.gethostbyname(hostname)
        except socket.gaierror:
            return False, f"Workspace hostname '{hostname}' could not be resolved. Please verify the URL is correct."

        # Make a lightweight request to check if workspace responds
        async with httpx.AsyncClient(timeout=WORKSPACE_CHECK_TIMEOUT) as client:
            # Try to access the workspace - even a 401/403 means it exists
            response = await client.head(url, follow_redirects=True)

            # Any response (even 401/403/404) means the server exists
            # We just want to verify it's not a completely fake URL
            logger.debug(
                "Workspace reachability check",
                url=url,
                status_code=response.status_code,
            )
            return True, ""

    except httpx.TimeoutException:
        return False, f"Connection to workspace '{url}' timed out. Please verify the URL is correct and accessible."
    except httpx.ConnectError as e:
        error_str = str(e).lower()
        if "name or service not known" in error_str or "nodename nor servname" in error_str:
            return False, f"Workspace hostname could not be resolved. Please verify the URL '{url}' is correct."
        if "connection refused" in error_str:
            return False, f"Connection to workspace '{url}' was refused. Please verify the URL is correct."
        return False, f"Could not connect to workspace '{url}': {e}"
    except httpx.RequestError as e:
        return False, f"Error connecting to workspace '{url}': {e}"
    except Exception as e:
        logger.warning("Unexpected error during workspace reachability check", url=url, error=str(e))
        # Don't fail on unexpected errors - let the SDK handle it
        return True, ""


async def get_workspace_url(
    x_workspace_url: str = Header(
        ...,
        alias="X-Workspace-URL",
        description="<small>*HTTPS URL of Databricks workspace*</small>",
    ),
) -> str:
    """
    Extract and validate the Databricks workspace URL from header.

    Validates that the URL:
    1. Is not empty
    2. Uses HTTPS protocol
    3. Matches a valid Databricks workspace pattern (Azure, AWS, or GCP)
    4. Is reachable (workspace exists and responds)

    Parameters
    ----------
    x_workspace_url : str
        Databricks workspace URL from X-Workspace-URL header

    Returns
    -------
    str
        Validated and normalized workspace URL (trailing slash removed)

    Raises
    ------
    HTTPException
        400 if header is missing, empty, or URL format is invalid
        502 if workspace is not reachable
    """
    if not x_workspace_url or not x_workspace_url.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Workspace-URL header is required",
        )

    # Normalize URL: strip whitespace and trailing slash
    url_normalized = x_workspace_url.strip().rstrip("/")

    # Validate URL uses HTTPS
    if not url_normalized.startswith("https://"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Workspace-URL must be a valid HTTPS URL",
        )

    # Validate URL matches Databricks patterns (Azure, AWS, or GCP)
    if not is_valid_databricks_url(url_normalized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid Databricks workspace URL format. "
                "Expected patterns: *.azuredatabricks.net (Azure), "
                "*.cloud.databricks.com (AWS), or *.gcp.databricks.com (GCP)"
            ),
        )

    # Check if workspace is reachable (fail fast for non-existent workspaces)
    is_reachable, error_message = await check_workspace_reachable(url_normalized)
    if not is_reachable:
        logger.warning(
            "Workspace reachability check failed",
            url=url_normalized,
            error=error_message,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=error_message,
        )

    return url_normalized


async def verify_apim_request(
    request: Request,
    x_apim_request: str
    | None = Header(
        None,
        alias="X-APIM-Request",
        description="Header set by Azure API Management to identify requests from APIM",
    ),
) -> bool:
    """
    Verify request is coming from Azure API Management.

    This is optional - if you want to enforce APIM-only access, configure
    APIM to add a secret header (X-APIM-Secret) and validate it here.

    For now, this just logs whether the request came through APIM.

    Parameters
    ----------
    request : Request
        FastAPI request object
    x_apim_request : str | None
        Optional header indicating request came through APIM

    Returns
    -------
    bool
        True if request is from APIM

    Notes
    -----
    To enable strict APIM validation:
    1. Configure APIM to set policy: set-header name="X-APIM-Secret" value="{{secret}}"
    2. Set APIM_SECRET environment variable in Web App
    3. Uncomment validation code below
    """
    # Optional: Validate APIM secret header
    # apim_secret = os.getenv("APIM_SECRET")
    # x_apim_secret = request.headers.get("X-APIM-Secret")
    # if apim_secret and x_apim_secret != apim_secret:
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="Direct access not allowed. Please use API Management.",
    #     )

    is_from_apim = x_apim_request is not None
    logger.debug(f"Request from APIM: {is_from_apim}")
    return is_from_apim
