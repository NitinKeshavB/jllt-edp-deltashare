# DeltaShare API Logging Configuration

This document explains the comprehensive logging setup for the DeltaShare API, which supports multiple logging destinations.

## Overview

The DeltaShare API uses **loguru** for structured logging with three configurable sinks:

1. **Console (stdout)** - Always enabled for development and debugging
2. **Azure Blob Storage** - Optional, for long-term log retention
3. **Azure PostgreSQL** - Optional, for critical logs and alerting

## Quick Start

### 1. Install Dependencies

```bash
# Install with Azure logging support
pip install -e ".[azure]"
```

### 2. Configure Environment Variables

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# Enable Azure Blob Storage logging
ENABLE_BLOB_LOGGING=true
AZURE_STORAGE_ACCOUNT_URL=https://yourstorageaccount.blob.core.windows.net
AZURE_STORAGE_LOGS_CONTAINER=deltashare-logs

# Enable PostgreSQL logging for critical logs
ENABLE_POSTGRESQL_LOGGING=true
POSTGRESQL_CONNECTION_STRING=postgresql://user:password@yourserver.postgres.database.azure.com:5432/deltashare_db?sslmode=require
POSTGRESQL_MIN_LOG_LEVEL=WARNING
```

### 3. Run the Application

```bash
make run-dev
```

## Logging Destinations

### Console Logging (Always On)

All logs are output to stdout with color-coded formatting:

```
2026-01-02 10:15:30.123 | INFO     | dbrx_api.routes_share:create_share:192 | Creating share | {"share_name": "my_share", "method": "POST", "path": "/shares/my_share"}
```

### Azure Blob Storage (Optional)

**Purpose**: Long-term retention, compliance, audit trails

**Storage Structure**:
```
deltashare-logs/
├── 2026/
│   ├── 01/
│   │   ├── 02/
│   │   │   ├── 10/
│   │   │   │   ├── log_20260102_101530_000000.json
│   │   │   │   ├── log_20260102_101545_123456.json
```

**Log Format**: JSON with full context
```json
{
  "timestamp": "2026-01-02T10:15:30.123456+00:00",
  "level": "INFO",
  "logger": "dbrx_api.routes_share",
  "function": "create_share",
  "line": 192,
  "message": "Creating share",
  "extra": {
    "share_name": "my_share",
    "method": "POST",
    "path": "/shares/my_share"
  }
}
```

**Authentication**: Uses Azure Managed Identity (recommended for production)

### PostgreSQL Database (Optional)

**Purpose**: Quick querying, alerting, monitoring dashboards

**Schema**: Auto-created on first run
```sql
CREATE TABLE application_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    level VARCHAR(20) NOT NULL,
    logger_name VARCHAR(255),
    function_name VARCHAR(255),
    line_number INTEGER,
    message TEXT NOT NULL,
    extra_data JSONB,
    exception_type VARCHAR(255),
    exception_value TEXT,
    exception_traceback TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Minimum Log Level**: Configurable (default: WARNING)
- Only logs at or above the specified level are stored
- Reduces database storage for high-volume applications

**Indexes**: Optimized for common queries
- Timestamp (descending)
- Log level
- Created at (descending)
- Extra data (GIN index for JSONB queries)

## Configuration Options

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ENABLE_BLOB_LOGGING` | No | `false` | Enable Azure Blob Storage logging |
| `AZURE_STORAGE_ACCOUNT_URL` | If blob enabled | - | Azure Storage Account URL |
| `AZURE_STORAGE_LOGS_CONTAINER` | No | `deltashare-logs` | Blob container name |
| `ENABLE_POSTGRESQL_LOGGING` | No | `false` | Enable PostgreSQL logging |
| `POSTGRESQL_CONNECTION_STRING` | If PG enabled | - | PostgreSQL connection string |
| `POSTGRESQL_LOG_TABLE` | No | `application_logs` | Table name for logs |
| `POSTGRESQL_MIN_LOG_LEVEL` | No | `WARNING` | Minimum level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |

## Log Levels

| Level | Priority | Use Case | Stored in Blob | Stored in DB (default) |
|-------|----------|----------|----------------|------------------------|
| TRACE | 0 | Very detailed debugging | ✅ (if enabled) | ❌ |
| DEBUG | 1 | Debugging information | ✅ (if enabled) | ❌ |
| INFO | 2 | General information | ✅ | ❌ |
| SUCCESS | 3 | Success confirmations | ✅ | ❌ |
| WARNING | 4 | Warning messages | ✅ | ✅ |
| ERROR | 5 | Error messages | ✅ | ✅ |
| CRITICAL | 6 | Critical errors | ✅ | ✅ |

## Querying Logs

### Azure Blob Storage

Use Azure Storage Explorer or Azure CLI:

```bash
# List logs for a specific date
az storage blob list \
  --account-name yourstorageaccount \
  --container-name deltashare-logs \
  --prefix "2026/01/02/" \
  --output table

# Download logs
az storage blob download \
  --account-name yourstorageaccount \
  --container-name deltashare-logs \
  --name "2026/01/02/10/log_20260102_101530_000000.json" \
  --file local_log.json
```

### PostgreSQL

```sql
-- Get all errors from last 24 hours
SELECT
    timestamp,
    level,
    function_name,
    message,
    extra_data
FROM application_logs
WHERE level IN ('ERROR', 'CRITICAL')
  AND timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Search logs by share name
SELECT
    timestamp,
    level,
    message,
    extra_data->>'share_name' as share_name
FROM application_logs
WHERE extra_data->>'share_name' = 'my_share'
ORDER BY timestamp DESC
LIMIT 100;

-- Count logs by level
SELECT
    level,
    COUNT(*) as count
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY level
ORDER BY count DESC;

-- Get recent exceptions
SELECT
    timestamp,
    logger_name,
    function_name,
    exception_type,
    exception_value,
    exception_traceback
FROM application_logs
WHERE exception_type IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10;
```

## Azure Setup

### 1. Create Azure Storage Account

```bash
# Create resource group
az group create --name deltashare-rg --location eastus

# Create storage account
az storage account create \
  --name deltasharelogs \
  --resource-group deltashare-rg \
  --location eastus \
  --sku Standard_LRS

# Get account URL
az storage account show \
  --name deltasharelogs \
  --resource-group deltashare-rg \
  --query "primaryEndpoints.blob" \
  --output tsv
```

### 2. Create Azure PostgreSQL Database

```bash
# Create PostgreSQL server
az postgres flexible-server create \
  --name deltashare-db-server \
  --resource-group deltashare-rg \
  --location eastus \
  --admin-user dbadmin \
  --admin-password 'YourSecurePassword!' \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --version 14

# Create database
az postgres flexible-server db create \
  --resource-group deltashare-rg \
  --server-name deltashare-db-server \
  --database-name deltashare_db

# Configure firewall (allow Azure services)
az postgres flexible-server firewall-rule create \
  --resource-group deltashare-rg \
  --name deltashare-db-server \
  --rule-name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0
```

### 3. Configure Managed Identity (Recommended)

```bash
# Enable system-assigned managed identity on App Service
az webapp identity assign \
  --name your-app-name \
  --resource-group deltashare-rg

# Grant Storage Blob Data Contributor role
PRINCIPAL_ID=$(az webapp identity show \
  --name your-app-name \
  --resource-group deltashare-rg \
  --query principalId \
  --output tsv)

az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/deltashare-rg/providers/Microsoft.Storage/storageAccounts/deltasharelogs
```

## Performance Considerations

### Azure Blob Storage
- **Async writes**: Logs are written asynchronously to avoid blocking API requests
- **Cost**: ~$0.02 per GB/month for storage
- **Partitioning**: Logs are organized by date/time for efficient querying

### PostgreSQL
- **Connection pooling**: Uses asyncpg with connection pool (2-10 connections)
- **Async inserts**: Non-blocking database writes
- **Selective logging**: Only WARNING and above by default to reduce load
- **Indexes**: Optimized for common query patterns

## Troubleshooting

### Logs not appearing in Azure Blob Storage

1. Check managed identity permissions:
```bash
az role assignment list --assignee <principal-id>
```

2. Verify storage account URL in `.env`

3. Check application logs for errors:
```
Failed to initialize Azure Blob Storage logging: ...
```

### Logs not appearing in PostgreSQL

1. Test connection string:
```bash
psql "postgresql://user:password@server.postgres.database.azure.com:5432/deltashare_db?sslmode=require"
```

2. Check log level configuration (only WARNING+ by default)

3. Verify firewall rules allow your IP/Azure services

### High database costs

- Increase `POSTGRESQL_MIN_LOG_LEVEL` to `ERROR` or `CRITICAL`
- Implement log rotation/archival:
```sql
-- Archive old logs
INSERT INTO application_logs_archive
SELECT * FROM application_logs
WHERE timestamp < NOW() - INTERVAL '30 days';

-- Delete archived logs
DELETE FROM application_logs
WHERE timestamp < NOW() - INTERVAL '30 days';
```

## Best Practices

1. **Enable blob logging in production** for compliance and audit trails
2. **Use PostgreSQL for critical logs only** to control costs
3. **Set up alerts** based on ERROR/CRITICAL logs in PostgreSQL
4. **Regular cleanup**: Archive/delete old blob logs after retention period
5. **Monitor costs**: Use Azure Cost Management to track logging expenses
6. **Test locally**: Use `ENABLE_BLOB_LOGGING=false` for development

## Example Monitoring Query

```sql
-- Alert: More than 10 errors in last 5 minutes
SELECT
    COUNT(*) as error_count,
    array_agg(DISTINCT function_name) as affected_functions
FROM application_logs
WHERE level = 'ERROR'
  AND timestamp > NOW() - INTERVAL '5 minutes'
HAVING COUNT(*) > 10;
```
