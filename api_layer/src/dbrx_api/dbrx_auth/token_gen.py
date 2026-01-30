"""Module for generating Databricks authentication token.

NOTE: This module only handles token generation (OAuth2 Client Credentials flow).
Token caching is handled by the TokenManager class in token_manager.py, which
provides thread-safe in-memory caching stored in FastAPI's app.state.
"""

import base64
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Tuple

import requests
from pydantic import ValidationError

from dbrx_api.settings import Settings

env_file = Path(__file__).parent.parent.parent.parent / ".env"


class CustomError(Exception):
    """Custom exception for token generation failures."""


def get_auth_token(exec_time_utc: datetime) -> Tuple[str, datetime]:
    """
    Generate an authentication token for Databricks API.

    Parameters
    ----------
    exec_time_utc : datetime
        Current execution time in UTC

    Returns
    -------
    Tuple[str, datetime]
        A tuple containing:
        - access_token: The OAuth access token
        - expires_at_utc: Expiration time as datetime object in UTC

    Raises
    ------
    CustomError
        If token generation fails for any reason

    """
    try:
        # NOTE: Token caching is handled by TokenManager class (in-memory, thread-safe)
        # This function only generates new tokens - no caching logic here

        try:
            settings = Settings(_env_file=env_file if env_file.exists() else None)
        except ValidationError as exc:
            raise CustomError(
                "Missing required environment variables: client_id, client_secret, or account_id. "
                "Ensure these are set in Azure App Service Configuration or .env file."
            ) from exc

        client_id = settings.client_id
        client_secret = settings.client_secret
        account_id = settings.account_id

        url = f"https://accounts.azuredatabricks.net/oidc/accounts/{account_id}/v1/token"

        # Prepare request payload
        payload = {"grant_type": "client_credentials", "scope": "all-apis"}

        # Encode credentials for Basic authentication
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {"Authorization": f"Basic {encoded_credentials}"}

        # Get current UTC time
        created_at_utc = datetime.now(timezone.utc)

        # Send token request
        response = requests.post(url, headers=headers, data=payload, timeout=30)

        # Check response status
        if response.status_code != 200:
            raise CustomError(f"Token request failed with status {response.status_code}: " f"{response.text}")

        # Parse response
        try:
            token_data = response.json()
        except json.JSONDecodeError as e:
            raise CustomError(f"Failed to parse token response as JSON: {e}") from e

        # Extract token information
        access_token = token_data.get("access_token")
        token_expiry = token_data.get("expires_in", 3600)

        if not access_token:
            raise CustomError("Access token not found in response")

        # Calculate expiration time in UTC
        expires_at_utc = created_at_utc + timedelta(seconds=token_expiry)

        # NOTE: Token caching is handled by TokenManager (in-memory)
        # No need to store in environment variables or .env file

        return access_token, expires_at_utc

    except requests.exceptions.RequestException as e:
        raise CustomError(f"Network error during token request: {e}") from e
    except Exception as e:
        if isinstance(e, CustomError):
            raise
        raise CustomError(f"Unexpected error during token generation: {e}") from e
