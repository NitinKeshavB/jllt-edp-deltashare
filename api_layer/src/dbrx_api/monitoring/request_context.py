"""Request context middleware for logging."""
import json
from contextvars import ContextVar
from typing import Any
from typing import Callable
from typing import Optional

from fastapi import Request
from fastapi import Response
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

# Context variables to store request-specific data
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")
client_ip_ctx: ContextVar[str] = ContextVar("client_ip", default="")
user_identity_ctx: ContextVar[str] = ContextVar("user_identity", default="")
user_agent_ctx: ContextVar[str] = ContextVar("user_agent", default="")
request_path_ctx: ContextVar[str] = ContextVar("request_path", default="")

# Maximum size for request/response body logging (to avoid memory issues)
MAX_BODY_LOG_SIZE = 10000  # 10KB limit


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware to capture and log request context information."""

    async def dispatch(self, request: Request, call_next: Callable) -> Any:
        """
        Capture request context and add to logging.

        Captures:
        - Request ID (from header or generated)
        - Client IP (real IP from Azure headers or direct)
        - User identity (from Azure AD, API key, or custom auth)
        - User agent (browser/client info)
        - Request path and method
        - Request body (for POST/PUT/PATCH)
        - Response body
        """
        # Generate or get request ID
        request_id = request.headers.get("X-Request-ID", self._generate_request_id())
        request_id_ctx.set(request_id)

        # Get real client IP (Azure Web App headers)
        client_ip = self._get_client_ip(request)
        client_ip_ctx.set(client_ip)

        # Get user identity (multiple sources)
        user_identity = self._get_user_identity(request)
        user_identity_ctx.set(user_identity)

        # Get user agent
        user_agent = request.headers.get("User-Agent", "unknown")
        user_agent_ctx.set(user_agent)

        # Get request path
        request_path = f"{request.method} {request.url.path}"
        request_path_ctx.set(request_path)

        # Capture request body for all methods that might have one
        # Store in request state early so error handlers can access it
        request.state.request_body = None

        # Try to capture request body for methods that typically have bodies
        # Also check DELETE as some APIs use DELETE with body
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            request_body = await self._get_request_body(request)
            request.state.request_body = request_body

        # Configure logger with context
        with logger.contextualize(
            request_id=request_id,
            client_ip=client_ip,
            user_identity=user_identity,
            user_agent=user_agent,
            request_path=request_path,
            referer=request.headers.get("Referer", "direct"),
            origin=request.headers.get("Origin", "unknown"),
        ):
            # Process request and capture timing
            import time

            start_time = time.time()
            response = await call_next(request)
            duration_ms = (time.time() - start_time) * 1000

            # Capture response body
            response_body, response = await self._capture_response_body(response)

            # Check if response contains sensitive information (for status 200)
            if response.status_code == 200 and response_body is not None:
                if self._contains_sensitive_information(response_body):
                    # Don't log response body for 200 status if it contains sensitive info
                    response_body = None

            # Log request and response together in a single entry
            # Use INFO level for normal requests (won't go to PostgreSQL with min_level=WARNING)
            # Errors/warnings are logged separately at WARNING/ERROR level and will include request/response bodies
            logger.info(
                f"{request.method} {request.url.path} - {response.status_code}",
                # Request details
                method=request.method,
                path=request.url.path,
                query_params=str(request.query_params),
                # Additional structured fields for external tables
                event_type="http_request",
                http_method=request.method,
                url_path=str(request.url.path),
                url_query=str(request.query_params) if request.query_params else None,
                http_version=request.scope.get("http_version", "1.1"),
                content_type=request.headers.get("Content-Type"),
                content_length=request.headers.get("Content-Length"),
                request_body=getattr(request.state, "request_body", None),
                # Response details
                status_code=response.status_code,
                http_status=response.status_code,
                response_time_ms=round(duration_ms, 2),
                response_content_type=response.headers.get("content-type"),
                response_content_length=response.headers.get("content-length"),
                response_body=response_body,
            )

            return response

    async def _get_request_body(self, request: Request) -> Optional[dict]:
        """
        Read and cache the request body with timeout to prevent hanging.

        Handles cases where body might have been consumed already by checking
        if it's available in request state or reading it directly.

        Returns:
            Parsed JSON body or None if not JSON/empty
        """
        try:
            import asyncio

            # Check if body was already read and cached
            if hasattr(request.state, "_body"):
                body = request.state._body
            else:
                # Read body with timeout to prevent hanging on large/slow bodies
                try:
                    body = await asyncio.wait_for(request.body(), timeout=2.0)
                    # Cache it for potential reuse
                    request.state._body = body
                except asyncio.TimeoutError:
                    # Body read timed out - return error indicator
                    return {"_error": "Request body read timeout (>2s)"}
                except RuntimeError:
                    # Body was already consumed, try to get from request state
                    body = getattr(request.state, "_body", None)
                    if body is None:
                        return None

            if not body:
                return None

            content_type = request.headers.get("Content-Type", "")
            # Accept various JSON content types
            if "application/json" not in content_type.lower():
                # If not JSON, return as string representation (truncated)
                try:
                    body_str = body.decode("utf-8", errors="replace")
                    if len(body_str) > 200:
                        return {
                            "_truncated": True,
                            "_size": len(body_str),
                            "_preview": body_str[:200],
                            "_content_type": content_type,
                        }
                    return {"_raw": body_str, "_content_type": content_type}
                except Exception:
                    return {"_error": "Failed to decode request body", "_content_type": content_type}

            # Truncate if too large (before parsing to avoid memory issues)
            if len(body) > MAX_BODY_LOG_SIZE:
                try:
                    preview = body[:1000].decode("utf-8", errors="replace")
                    return {"_truncated": True, "_size": len(body), "_preview": preview}
                except Exception:
                    return {"_truncated": True, "_size": len(body), "_preview": "[Binary data]"}

            # Parse JSON
            return json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Return error info instead of None so we know parsing failed
            return {"_error": "Failed to parse request body", "_error_detail": str(e)}
        except Exception:
            # Log but don't fail - return None to indicate body couldn't be read
            return None

    async def _capture_response_body(self, response: Response) -> tuple[Optional[dict], Response]:
        """
        Capture and return the response body without breaking the response.
        Includes timeout to prevent hanging on large responses.

        Returns:
            Tuple of (parsed body or None, new response)
        """
        try:
            pass

            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return None, response

            # Read the response body with timeout to prevent hanging
            body_bytes = b""
            import time

            start_time = time.time()
            timeout = 3.0  # 3 second timeout for response body

            try:
                async for chunk in response.body_iterator:
                    # Check timeout on each chunk
                    if time.time() - start_time > timeout:
                        return {"_error": "Response body read timeout (>3s)"}, response

                    body_bytes += chunk
                    # Limit total size to prevent memory issues
                    if len(body_bytes) > MAX_BODY_LOG_SIZE * 2:  # Allow 2x for safety
                        break
            except Exception:
                # If reading body fails, return error indicator
                return {"_error": "Failed to read response body"}, response

            if not body_bytes:
                return None, Response(
                    content=body_bytes,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )

            # Truncate if too large for logging
            if len(body_bytes) > MAX_BODY_LOG_SIZE:
                response_body = {
                    "_truncated": True,
                    "_size": len(body_bytes),
                    "_preview": body_bytes[:1000].decode("utf-8", errors="replace"),
                }
            else:
                try:
                    response_body = json.loads(body_bytes)
                except json.JSONDecodeError:
                    response_body = {"_raw": body_bytes.decode("utf-8", errors="replace")[:1000]}

            # Create new response with the same body
            new_response = Response(
                content=body_bytes,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )

            return response_body, new_response
        except Exception:
            # If capture fails, return original response without body logging
            return None, response

    def _get_client_ip(self, request: Request) -> str:
        """
        Get real client IP address.

        Azure Web App provides these headers:
        - X-Forwarded-For: Original client IP
        - X-Azure-ClientIP: Client IP from Azure
        - X-Forwarded-Host: Original host
        """
        # Try Azure-specific headers first
        client_ip = request.headers.get("X-Azure-ClientIP")
        if client_ip:
            return client_ip

        # Try standard forwarded headers
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # X-Forwarded-For can contain multiple IPs, take the first one
            return forwarded_for.split(",")[0].strip()

        # Fallback to direct client
        if request.client:
            return request.client.host

        return "unknown"

    def _get_user_identity(self, request: Request) -> str:
        """
        Get user identity from multiple sources.

        Priority order:
        1. Azure AD authentication (Easy Auth headers)
        2. Custom authorization header (Bearer token)
        3. API key header
        4. Client certificate (for mTLS)
        5. Anonymous
        """
        # Azure AD / Easy Auth headers
        # When Azure App Service Authentication is enabled, these headers are automatically added
        azure_user_principal = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")
        if azure_user_principal:
            azure_user_id = request.headers.get("X-MS-CLIENT-PRINCIPAL-ID", "")
            return f"{azure_user_principal} ({azure_user_id})" if azure_user_id else azure_user_principal

        # Check for Bearer token (you'd decode this in real implementation)
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            # In production, decode JWT token to get user info
            # For now, just indicate it's a bearer token user
            token_preview = auth_header[7:27] + "..."  # First 20 chars
            return f"bearer_token:{token_preview}"

        # Check for API key header
        api_key = request.headers.get("X-API-Key")
        if api_key:
            # In production, look up API key owner in database
            key_preview = api_key[:8] + "..." if len(api_key) > 8 else api_key
            return f"api_key:{key_preview}"

        # Check for client certificate (mTLS)
        client_cert = request.headers.get("X-ARR-ClientCert")  # Azure App Service header
        if client_cert:
            return "mtls:certificate_auth"

        # Anonymous access
        return "anonymous"

    def _contains_sensitive_information(self, body: Any) -> bool:
        """
        Check if response body contains sensitive information.

        Detects:
        - Passwords, secrets, tokens, API keys
        - JWT tokens
        - Bearer tokens
        - Private keys
        - Credentials

        Args:
            body: Response body (dict, list, str, or other)

        Returns:
            True if sensitive information is detected, False otherwise
        """
        if body is None:
            return False

        import json
        import re

        # Convert to string for pattern matching
        try:
            if isinstance(body, (dict, list)):
                body_str = json.dumps(body, default=str).lower()
            else:
                body_str = str(body).lower()
        except Exception:
            return False

        # Patterns that indicate sensitive information
        sensitive_patterns = [
            r"password[=:]\s*['\"]?[^\s'\"]+",  # password=xxx or password: xxx
            r"secret[=:]\s*['\"]?[^\s'\"]+",  # secret=xxx
            r"token[=:]\s*['\"]?[^\s'\"]+",  # token=xxx (but not bearer_token which is already sanitized)
            r"apikey[=:]\s*['\"]?[^\s'\"]+",  # apikey=xxx
            r"api_key[=:]\s*['\"]?[^\s'\"]+",  # api_key=xxx
            r"access_token[=:]\s*['\"]?[^\s'\"]+",  # access_token=xxx
            r"refresh_token[=:]\s*['\"]?[^\s'\"]+",  # refresh_token=xxx
            r"authorization[=:]\s*['\"]?[^\s'\"]+",  # authorization=xxx
            r"credential[=:]\s*['\"]?[^\s'\"]+",  # credential=xxx
            r"private[_\s]?key[=:]\s*['\"]?[^\s'\"]+",  # private_key=xxx
            r"-----BEGIN[^\n]+PRIVATE KEY-----",  # Private key block
            r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",  # JWT tokens
            r"bearer\s+[A-Za-z0-9_-]{20,}",  # Bearer tokens (long strings)
            r"x-api-key[=:]\s*['\"]?[^\s'\"]+",  # x-api-key header value
        ]

        # Check for sensitive patterns
        for pattern in sensitive_patterns:
            if re.search(pattern, body_str, re.IGNORECASE):
                return True

        # Check for common sensitive field names in dicts
        if isinstance(body, dict):
            sensitive_keys = [
                "password",
                "secret",
                "token",
                "apikey",
                "api_key",
                "access_token",
                "refresh_token",
                "authorization",
                "credential",
                "private_key",
                "privatekey",
                "client_secret",
                "clientsecret",
            ]
            for key in body.keys():
                if isinstance(key, str) and any(sensitive in key.lower() for sensitive in sensitive_keys):
                    # Check if the value is not already redacted
                    value = str(body[key]).lower()
                    if "redacted" not in value and "***" not in value and len(value) > 5:
                        return True

        return False

    def _generate_request_id(self) -> str:
        """Generate a unique request ID."""
        import uuid

        return str(uuid.uuid4())


def get_request_context() -> dict:
    """
    Get current request context for logging.

    Returns:
        Dictionary with request context variables
    """
    return {
        "request_id": request_id_ctx.get(),
        "client_ip": client_ip_ctx.get(),
        "user_identity": user_identity_ctx.get(),
        "user_agent": user_agent_ctx.get(),
        "request_path": request_path_ctx.get(),
    }
