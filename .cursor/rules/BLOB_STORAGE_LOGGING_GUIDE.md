# Azure Blob Storage Logging Guide

## Overview

This guide explains how to configure Azure Blob Storage logging for the DeltaShare API application. The application supports three authentication methods for writing logs to Azure Blob Storage.

## Authentication Methods (Priority Order)

The application tries authentication methods in this order:

1. **SAS URL** (Highest Priority) - Recommended for production
2. **Connection String** - Good for development/testing
3. **Managed Identity** - Best for Azure Web Apps with proper RBAC

## Method 1: SAS URL (Recommended)

### Advantages
- ✅ Simple to configure
- ✅ No need for RBAC role assignments
- ✅ Can be scoped to specific container
- ✅ Can set expiration time
- ✅ Works in all environments

### Configuration

1. **Generate SAS Token in Azure Portal:**
   - Go to Azure Portal → Storage Account → Shared access signature
   - Select permissions: **Read**, **Add**, **Create**, **Write**, **List** (or use `racwdli`)
   - Set expiration (recommend 1 year for production)
   - Generate SAS token and connection string
   - Copy the **SAS URL** (not just the token)

2. **Add to `.env` file:**
   ```bash
   ENABLE_BLOB_LOGGING=true
   AZURE_STORAGE_ACCOUNT_URL=https://<account>.blob.core.windows.net/
   AZURE_STORAGE_LOGS_CONTAINER=deltashare-logs
   AZURE_STORAGE_SAS_URL=https://<account>.blob.core.windows.net/deltashare-logs?sp=racwdli&st=...&se=...&spr=https&sv=2024-11-04&sr=c&sig=...
   ```

3. **SAS URL Formats:**
   - **Container-level** (recommended): `https://<account>.blob.core.windows.net/<container>?<sas-token>`
   - **Account-level**: `https://<account>.blob.core.windows.net?<sas-token>`

### Required Permissions
- `r` - Read
- `a` - Add
- `c` - Create
- `w` - Write
- `l` - List

Or use `racwdli` for all permissions.

## Method 2: Connection String

### Advantages
- ✅ Simple to configure
- ✅ No expiration
- ✅ Works in all environments

### Disadvantages
- ⚠️ Full account access (less secure than SAS URL)
- ⚠️ Cannot be scoped to specific container

### Configuration

1. **Get Connection String:**
   - Azure Portal → Storage Account → Access keys
   - Copy **Connection string** (key1 or key2)

2. **Add to `.env` file:**
   ```bash
   ENABLE_BLOB_LOGGING=true
   AZURE_STORAGE_ACCOUNT_URL=https://<account>.blob.core.windows.net/
   AZURE_STORAGE_LOGS_CONTAINER=deltashare-logs
   AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
   ```

### Important Notes
- Connection string must include `AccountKey=`
- Remove any quotes around the connection string in `.env`
- Connection string provides full account access

## Method 3: Managed Identity (Azure Web App Only)

### Advantages
- ✅ No secrets to manage
- ✅ Automatic credential rotation
- ✅ Most secure option

### Disadvantages
- ⚠️ Requires RBAC role assignment
- ⚠️ Only works in Azure Web App environment
- ⚠️ More complex setup

### Configuration

1. **Enable Managed Identity:**
   - Azure Portal → Web App → Identity
   - Enable **System assigned managed identity**

2. **Grant RBAC Role:**
   - Azure Portal → Storage Account → Access control (IAM)
   - Add role assignment:
     - Role: **Storage Blob Data Contributor**
     - Assign access to: **Managed identity**
     - Select: Your Web App's managed identity

3. **Add to `.env` or App Settings:**
   ```bash
   ENABLE_BLOB_LOGGING=true
   AZURE_STORAGE_ACCOUNT_URL=https://<account>.blob.core.windows.net/
   AZURE_STORAGE_LOGS_CONTAINER=deltashare-logs
   # No connection string or SAS URL needed
   ```

## Environment-Specific Behavior

### Local Development
- Blob logging is **automatically disabled** regardless of `ENABLE_BLOB_LOGGING` setting
- This prevents accidental log writes during local development
- To test locally, you must modify `main.py` (not recommended)

### Azure Web App
- Blob logging respects `ENABLE_BLOB_LOGGING` setting
- Uses App Settings (Configuration → Application settings)
- Supports all three authentication methods

## Verification

### 1. Test Upload
```bash
curl -X POST http://localhost:8000/health/logging/test
```

This forces a test log upload and returns:
- `success`: Whether upload succeeded
- `blob_name`: Name of test blob
- `error`: Error message if failed
- `details`: Upload count, failed count, last_error

### 2. Check Application Logs
Look for these messages in application logs:
- `"Initializing Azure Blob Storage client using SAS URL"`
- `"Container name extracted from SAS URL: <container>"`
- `"SAS token valid. Expires in: ..."`
- `"✓ Successfully uploaded log to blob storage"`

### 3. Check Azure Portal
- Azure Portal → Storage Account → Containers → `deltashare-logs`
- Verify log files are being created
- Log files are organized by date: `YYYY/MM/DD/HH/log_*.json`

## Troubleshooting

### Issue: "AuthorizationPermissionMismatch" (Web App / Managed Identity)
**Cause:** The identity used to write to Blob Storage does not have the right permissions.

**If using Managed Identity (no SAS URL):**
1. Azure Portal → Storage Account → **Access control (IAM)** → **Add role assignment**
2. Role: **Storage Blob Data Contributor**
3. Assign access to: **User, group, or service principal**
4. Select your **Web App** (its system-assigned managed identity)
5. Save. Changes can take a few minutes to apply.

**If using SAS URL:**
1. Ensure the SAS URL includes **write** permissions: `sp=racwdli` (or at least `w` and `c`)
2. Check SAS token expiration (`se=` in the URL)
3. Regenerate the SAS token in Azure Portal → Storage Account → Shared access signature
4. Update `AZURE_STORAGE_SAS_URL` in Web App Configuration (or `.env`)

**Note:** Permission errors are throttled in logs (once per 5 minutes) to avoid spam.

### Issue: "Container does not exist"
**Cause:** Container not created or no create permission

**Solutions:**
1. Create container manually in Azure Portal
2. Ensure SAS token has `c` (create) permission
3. Check container name matches `AZURE_STORAGE_LOGS_CONTAINER`

### Issue: No logs appearing in container
**Possible Causes:**
1. Blob logging disabled locally (expected behavior)
2. Client not initialized
3. Uploads failing silently
4. Wrong container name
5. SAS token expired

**Diagnosis:**
1. Call `POST /health/logging/test` to force a test upload and see success/error details
2. Check application logs for error messages
3. Verify container name in Azure Portal

### Issue: "SAS token appears to be EXPIRED"
**Solution:** Generate new SAS token with longer expiration

### Issue: Connection string appears invalid
**Check:**
- Connection string includes `AccountKey=`
- No quotes around connection string in `.env`
- Connection string is complete (should be ~200+ characters)

## Best Practices

1. **Use SAS URL for Production**
   - Set expiration to 1 year
   - Scope to specific container
   - Include only necessary permissions (`racwdli`)

2. **Use Connection String for Development**
   - Easier to configure
   - No expiration to manage
   - Store securely (not in git)

3. **Use Managed Identity for Azure Web Apps**
   - Most secure
   - No secrets in configuration
   - Automatic credential rotation

4. **Verify Logging**
   - Use `POST /health/logging/test` to verify blob storage is working
   - Monitor container size and costs

5. **Container Naming**
   - Use descriptive names: `deltashare-logs`
   - Separate containers per environment: `deltashare-logs-dev`, `deltashare-logs-prd`
   - Follow Azure naming conventions (lowercase, no special chars)

## Log File Structure

Logs are stored as JSON files with this structure:
```
{
  "timestamp": "2026-01-28T12:00:00.000000+00:00",
  "level": "INFO",
  "logger": "dbrx_api.monitoring.request_context",
  "function": "dispatch",
  "line": 97,
  "message": "GET /health - 200",
  "http": {
    "request_id": "abc123",
    "method": "GET",
    "url_path": "/health",
    "status_code": 200,
    "response_time_ms": 5.23,
    "client_ip": "192.168.1.1",
    "user_identity": "user@example.com",
    "user_agent": "Mozilla/5.0...",
    "request_body": {...},
    "response_body": {...}
  },
  "extra": {...}
}
```

## Configuration Reference

### Environment Variables

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `ENABLE_BLOB_LOGGING` | Yes | Enable/disable blob logging | `true` |
| `AZURE_STORAGE_ACCOUNT_URL` | Yes | Storage account URL | `https://account.blob.core.windows.net/` |
| `AZURE_STORAGE_LOGS_CONTAINER` | Yes | Container name | `deltashare-logs` |
| `AZURE_STORAGE_SAS_URL` | No* | SAS URL (highest priority) | `https://account.blob.core.windows.net/container?sp=...` |
| `AZURE_STORAGE_CONNECTION_STRING` | No* | Connection string (fallback) | `DefaultEndpointsProtocol=https;...` |

*At least one authentication method (SAS URL, Connection String, or Managed Identity) is required.

## Support

For issues or questions:
1. Check application logs for detailed error messages
2. Use `POST /health/logging/test` to verify and get error details
3. Review this guide for common issues
