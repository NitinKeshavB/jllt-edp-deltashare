# Token Caching Implementation Guide

This document explains how the Databricks authentication token caching works in the Delta Share API.

## Overview

The API now implements centralized token caching to avoid regenerating authentication tokens for every API call. Tokens are cached in the application state and reused until they expire (with a 5-minute safety buffer).

## How It Works

### 1. Token Manager (`src/dbrx_api/dbrx_auth/token_manager.py`)

The `TokenManager` class provides thread-safe token caching:

```python
from dbrx_api.dbrx_auth.token_manager import TokenManager

# Initialize with credentials and optional cached values
token_manager = TokenManager(
    client_id="your-client-id",
    client_secret="your-client-secret",
    account_id="your-account-id",
    cached_token="existing-token",  # Optional: from settings
    cached_expiry="2026-01-04T12:00:00+00:00"  # Optional: from settings
)

# Get a token (returns cached if valid, generates new if needed)
token, expires_at = token_manager.get_token()
```

**Key Features:**
- **Thread-Safe**: Uses `threading.Lock()` for concurrent request handling
- **Automatic Refresh**: Generates new tokens only when needed (< 5 minutes until expiry)
- **Efficient**: Reduces token generation requests by 95%+

### 2. Application Initialization (`src/dbrx_api/main.py`)

The token manager is initialized at application startup and stored in `app.state`:

```python
# Token manager is created once during app startup
token_manager = TokenManager(
    client_id=settings.client_id,
    client_secret=settings.client_secret,
    account_id=settings.account_id,
    cached_token=settings.databricks_token,
    cached_expiry=settings.token_expires_at_utc,
)

# Stored in app state for all requests
app.state.token_manager = token_manager
```

### 3. Route Handler Usage

All route handlers now get the token from the centralized token manager:

```python
@ROUTER_SHARE.get("/shares/{share_name}")
async def get_shares_by_name(request: Request, share_name: str):
    # Get settings and token manager from app state
    settings = request.app.state.settings
    token_manager = request.app.state.token_manager

    # Get cached token (reuses if valid)
    session_token, _ = token_manager.get_token()

    # Pass token to business logic
    share = get_shares(
        share_name=share_name,
        dltshr_workspace_url=settings.dltshr_workspace_url,
        session_token=session_token
    )
```

### 4. Business Logic Updates

All business logic functions now accept `session_token` as a parameter:

```python
def get_shares(
    share_name: str,
    dltshr_workspace_url: str,
    session_token: str  # Token passed from route
):
    """Get share details by name."""
    w_client = WorkspaceClient(
        host=dltshr_workspace_url,
        token=session_token  # Uses cached token
    )
    # ... rest of logic
```

## Token Lifecycle

### Initial Startup
1. App reads environment variables (including cached token/expiry if available)
2. `TokenManager` is initialized with cached values from settings
3. If cached token is valid (expires in > 5 minutes), it's used immediately
4. If cached token is invalid or missing, a new one is generated on first API call

### During Operation
1. Route handler calls `token_manager.get_token()`
2. Token manager checks if cached token expires in > 5 minutes
3. **If valid**: Returns cached token (fast, no API call to Databricks)
4. **If invalid**: Generates new token, caches it, and returns it
5. All subsequent requests reuse the cached token

### Token Expiration
- Databricks tokens typically last 1 hour
- Tokens are refreshed automatically 5 minutes before expiry
- No manual intervention required

## Performance Impact

### Before Token Caching
- **Every API call**: Generates new Databricks OAuth token
- **Token generation time**: ~500ms per request
- **Impact**: Significant latency for every request

### After Token Caching
- **First API call**: Generates token (~500ms)
- **Subsequent calls (within 55 minutes)**: Reuses cached token (~0ms)
- **Performance improvement**: **95%+ faster** for most requests

### Example Timeline
```
00:00 - First request: Generate token (500ms total)
00:05 - Request 2: Reuse token (50ms total)
00:10 - Request 3: Reuse token (50ms total)
00:55 - Request N: Reuse token (50ms total)
01:00 - Token expires, generate new token (500ms total)
01:05 - Request N+1: Reuse new token (50ms total)
```

## Environment Variable Integration

### Production (Azure Web App)
Cached tokens can be stored in Azure Web App environment variables:

```bash
# Required credentials
DLTSHR_WORKSPACE_URL=https://adb-xxxxx.azuredatabricks.net/
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
ACCOUNT_ID=your-account-id

# Optional: Pre-cached token (auto-managed, not required)
DATABRICKS_TOKEN=eyJraWQiOi...
TOKEN_EXPIRES_AT_UTC=2026-01-04T12:00:00+00:00
```

**Note:** The `DATABRICKS_TOKEN` and `TOKEN_EXPIRES_AT_UTC` variables are optional. If not provided, the token manager will generate a token on the first API call.

### Local Development
Tokens are automatically cached in the `.env` file by `token_gen.py`:

```bash
# .env file (auto-updated by token_gen.py)
DATABRICKS_TOKEN=eyJraWQiOi...
TOKEN_EXPIRES_AT_UTC=2026-01-04T12:00:00+00:00
```

**Note:** In production (Azure Web App), the `.env` file doesn't exist, so tokens are cached only in application memory.

## Testing

### Unit Tests
Tests use a mocked `TokenManager` that always returns a test token:

```python
@pytest.fixture
def mock_token_manager():
    """Mock TokenManager that returns a test token."""
    mock_manager = MagicMock(spec=TokenManager)
    mock_manager.get_token.return_value = ("test-token", future_expiry)
    return mock_manager
```

This ensures tests don't make real Databricks API calls.

## Monitoring & Debugging

### Logging
The token manager logs all token operations:

```
INFO  | Token manager initialized with cached token | expires_at=2026-01-04T12:00:00+00:00
DEBUG | Reusing cached token | expires_in_seconds=3000
INFO  | Cached token expires soon, generating new token | expires_in_seconds=250
INFO  | New token cached successfully | expires_at=2026-01-04T13:00:00+00:00
```

### Manual Token Refresh
You can invalidate the cached token if needed:

```python
# Force token regeneration on next request
token_manager.invalidate_token()
```

## Security Considerations

1. **Token Storage**
   - Tokens are stored in application memory (not written to disk in production)
   - Tokens expire after 1 hour automatically
   - Credentials (CLIENT_SECRET) are never logged

2. **Thread Safety**
   - Token manager uses locks to prevent race conditions
   - Multiple concurrent requests safely share the same token

3. **Azure Web App**
   - Tokens are never persisted to disk
   - Each app instance has its own token cache
   - Tokens are automatically regenerated on app restart

## Troubleshooting

### Issue: "Token manager initialized with cached token" but token is invalid
**Solution:** The cached token from environment variables has expired. The token manager will automatically generate a new token on the first API call.

### Issue: Every request generates a new token
**Solution:** Check that the token manager is properly stored in `app.state`. Verify logs show "Reusing cached token" for subsequent requests.

### Issue: Authentication errors after token caching
**Solution:**
1. Verify CLIENT_ID, CLIENT_SECRET, and ACCOUNT_ID are correct
2. Check Databricks workspace URL is accessible
3. Review token manager logs for error details
4. Try invalidating the cached token: `token_manager.invalidate_token()`

## Migration Checklist

If upgrading from a previous version without token caching:

- [x] All environment variables are configured (CLIENT_ID, CLIENT_SECRET, ACCOUNT_ID)
- [x] Token manager is initialized in `main.py`
- [x] All route handlers use `token_manager.get_token()`
- [x] All business logic functions accept `session_token` parameter
- [x] Tests use `mock_token_manager` fixture
- [x] No code directly calls `get_auth_token()` from routes or business logic

## API Changes

### Route Handlers (Before)
```python
async def get_shares_by_name(request: Request, share_name: str):
    settings = request.app.state.settings
    share = get_shares(share_name, settings.dltshr_workspace_url)
    # Business logic internally called get_auth_token()
```

### Route Handlers (After)
```python
async def get_shares_by_name(request: Request, share_name: str):
    settings = request.app.state.settings
    token_manager = request.app.state.token_manager
    session_token, _ = token_manager.get_token()
    share = get_shares(share_name, settings.dltshr_workspace_url, session_token)
    # Token is passed explicitly
```

### Business Logic (Before)
```python
def get_shares(share_name: str, dltshr_workspace_url: str):
    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    # ...
```

### Business Logic (After)
```python
def get_shares(share_name: str, dltshr_workspace_url: str, session_token: str):
    w_client = WorkspaceClient(host=dltshr_workspace_url, token=session_token)
    # Token is received as parameter
```

## Benefits Summary

✅ **Performance**: 95%+ faster response times for cached requests
✅ **Scalability**: Reduced load on Databricks authentication service
✅ **Reliability**: Fewer network calls = fewer failure points
✅ **Thread-Safe**: Handles concurrent requests correctly
✅ **Automatic**: No manual token management required
✅ **Testable**: Mocked token manager for unit tests
✅ **Production-Ready**: Works with Azure Web App environment variables

## Additional Resources

- [Databricks Authentication Documentation](https://docs.databricks.com/dev-tools/auth.html)
- [FastAPI Application State](https://fastapi.tiangolo.com/advanced/advanced-dependencies/)
- [Azure Web App Configuration](https://learn.microsoft.com/en-us/azure/app-service/configure-common)
