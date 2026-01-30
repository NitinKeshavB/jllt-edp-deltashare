# Azure Blob Storage Logging Guide

Complete guide to enable Azure Blob Storage logging with structured format for external tables and analytics.

## 🎯 What Gets Logged to Blob Storage

Every API request/response is captured as a structured JSON file with:

### Request Information
- ✅ **Timestamp**: ISO 8601 format with timezone
- ✅ **Request ID**: Unique identifier for request tracing
- ✅ **HTTP Method**: GET, POST, PUT, DELETE, etc.
- ✅ **URL Path**: `/shares/my_share`
- ✅ **Query Parameters**: `?limit=100&offset=0`
- ✅ **HTTP Version**: 1.1, 2.0, etc.
- ✅ **Content Type**: `application/json`
- ✅ **Content Length**: Request body size

### User Context
- ✅ **User Identity**: From Azure AD, JWT, API keys, certificates
- ✅ **Client IP**: Real IP from Azure headers
- ✅ **User Agent**: Browser/client information
- ✅ **Origin**: Where the request came from
- ✅ **Referer**: Previous page URL

### Response Information
- ✅ **HTTP Status**: 200, 404, 500, etc.
- ✅ **Response Time**: Duration in milliseconds
- ✅ **Response Content Type**: `application/json`
- ✅ **Response Content Length**: Response body size

### Application Logs
- ✅ **Log Level**: INFO, WARNING, ERROR, CRITICAL
- ✅ **Message**: Human-readable log message
- ✅ **Logger Name**: Which module logged it
- ✅ **Function/Line**: Code location
- ✅ **Exception Details**: Full stack traces for errors

---

## 📋 Prerequisites

1. **Azure Storage Account** (General Purpose v2)
2. **Managed Identity** OR **Storage Account Key**
3. **Network Access** from Azure Web App to Storage Account

---

## 🚀 Step 1: Create Azure Storage Account

### Option A: Azure Portal

1. Go to [Azure Portal](https://portal.azure.com) → Create Resource
2. Search **"Storage Account"**
3. Create with these settings:
   - **Name**: `deltasharelogsstorage` (must be globally unique)
   - **Region**: Same as your Web App
   - **Performance**: Standard (lower cost)
   - **Redundancy**: LRS (locally-redundant) for logs
   - **Blob public access**: Disabled (security)
4. Create the account

5. After creation, create a container:
   - Go to **Containers** → **+ Container**
   - **Name**: `deltashare-logs`
   - **Public access level**: Private
   - Create

### Option B: Azure CLI (Faster)

```bash
# Variables
RESOURCE_GROUP="your-resource-group"
LOCATION="eastus"
STORAGE_ACCOUNT="deltasharelogsstorage"  # Must be globally unique
CONTAINER_NAME="deltashare-logs"

# Create storage account
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access false

# Get storage account URL
STORAGE_URL=$(az storage account show \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query primaryEndpoints.blob \
  --output tsv)

echo "Storage URL: $STORAGE_URL"

# Create container
az storage container create \
  --name $CONTAINER_NAME \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login
```

---

## 🔐 Step 2: Configure Authentication

### Option A: Managed Identity (Recommended - Most Secure)

**1. Enable Managed Identity on Web App:**
```bash
WEB_APP_NAME="webagenticops"
RESOURCE_GROUP="your-resource-group"

# Enable system-assigned managed identity
az webapp identity assign \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP
```

**2. Grant Storage Access to Web App:**
```bash
# Get the managed identity principal ID
PRINCIPAL_ID=$(az webapp identity show \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query principalId \
  --output tsv)

# Get storage account resource ID
STORAGE_ID=$(az storage account show \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query id \
  --output tsv)

# Assign "Storage Blob Data Contributor" role
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

**3. Set Environment Variables (Managed Identity):**
```bash
az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    ENABLE_BLOB_LOGGING=true \
    AZURE_STORAGE_ACCOUNT_URL="https://deltasharelogsstorage.blob.core.windows.net" \
    AZURE_STORAGE_LOGS_CONTAINER="deltashare-logs"
```

### Option B: Storage Account Key (Simpler, Less Secure)

**1. Get Storage Account Key:**
```bash
STORAGE_KEY=$(az storage account keys list \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query '[0].value' \
  --output tsv)

echo "Storage Key: $STORAGE_KEY"
```

**2. Create Connection String:**
```bash
CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=$STORAGE_ACCOUNT;AccountKey=$STORAGE_KEY;EndpointSuffix=core.windows.net"
```

**3. Set Environment Variables (Account Key):**
```bash
az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    ENABLE_BLOB_LOGGING=true \
    AZURE_STORAGE_ACCOUNT_URL="https://deltasharelogsstorage.blob.core.windows.net" \
    AZURE_STORAGE_LOGS_CONTAINER="deltashare-logs"
    # Note: If using account key instead of managed identity,
    # you'd need to modify the handler to accept connection string
```

**💡 Recommendation:** Use Managed Identity (Option A) for production - it's more secure and doesn't require managing keys.

---

## 📁 Step 3: Understand Log Structure

### Blob Storage Organization

Logs are automatically partitioned by date/time for efficient querying:

```
deltashare-logs/
├── 2026/
│   ├── 01/
│   │   ├── 05/
│   │   │   ├── 14/
│   │   │   │   ├── log_20260105_140532_123456.json
│   │   │   │   ├── log_20260105_140533_234567.json
│   │   │   │   └── log_20260105_140534_345678.json
│   │   │   ├── 15/
│   │   │   └── 16/
│   │   ├── 06/
│   │   └── 07/
│   └── 02/
└── 2025/
```

**Partitioning scheme:** `YYYY/MM/DD/HH/log_YYYYMMDD_HHMMss_microseconds.json`

**Benefits:**
- ✅ Easy to query specific date ranges
- ✅ Efficient for external table partitioning
- ✅ Natural organization for time-series analysis
- ✅ Easy to archive/delete old logs

### JSON Log Format

Each log file contains structured JSON:

```json
{
  "timestamp": "2026-01-05T14:05:32.123456Z",
  "level": "INFO",
  "logger": "dbrx_api.monitoring.request_context",
  "function": "dispatch",
  "line": 88,
  "message": "Request completed",
  "extra": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "client_ip": "20.185.123.45",
    "user_identity": "john.doe@company.com (abc123)",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "request_path": "GET /shares/my_share",
    "referer": "https://example.com/dashboard",
    "origin": "https://example.com",
    "event_type": "request_completed",
    "http_method": "GET",
    "url_path": "/shares/my_share",
    "url_query": "limit=100",
    "http_version": "1.1",
    "content_type": "application/json",
    "content_length": "256",
    "http_status": 200,
    "response_time_ms": 45.23,
    "response_content_type": "application/json",
    "response_content_length": "1024"
  },
  "exception": null
}
```

**Key fields for analytics:**
- `timestamp`: When it happened
- `level`: Severity (INFO, WARNING, ERROR)
- `message`: What happened
- `extra.*`: All structured data (request/response details)

---

## 📊 Step 4: Query Logs with External Tables

### Option A: Azure Synapse Analytics (Serverless SQL)

**1. Create External Data Source:**
```sql
-- Create database
CREATE DATABASE DeltaShareAnalytics;
GO

USE DeltaShareAnalytics;
GO

-- Create master key for encryption
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';
GO

-- Create database scoped credential (if using account key)
CREATE DATABASE SCOPED CREDENTIAL BlobStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2021-06-08&ss=b&srt=sco&sp=rl&se=2027-01-01T00:00:00Z&st=2026-01-01T00:00:00Z&spr=https&sig=...';
GO

-- Or use managed identity (recommended)
CREATE DATABASE SCOPED CREDENTIAL BlobStorageCredential
WITH IDENTITY = 'Managed Identity';
GO

-- Create external data source
CREATE EXTERNAL DATA SOURCE DeltaShareLogs
WITH (
    LOCATION = 'https://deltasharelogsstorage.blob.core.windows.net/deltashare-logs',
    CREDENTIAL = BlobStorageCredential
);
GO
```

**2. Create External File Format:**
```sql
-- Define JSON format
CREATE EXTERNAL FILE FORMAT JsonFileFormat
WITH (
    FORMAT_TYPE = JSON
);
GO
```

**3. Create External Table:**
```sql
-- Create external table for logs
CREATE EXTERNAL TABLE ApiRequestLogs
(
    [timestamp] VARCHAR(50),
    [level] VARCHAR(20),
    [logger] VARCHAR(255),
    [function] VARCHAR(255),
    [line] INT,
    [message] VARCHAR(MAX),
    [extra] NVARCHAR(MAX),  -- JSON column
    [exception] NVARCHAR(MAX)  -- JSON column
)
WITH (
    LOCATION = '/',  -- Root of container
    DATA_SOURCE = DeltaShareLogs,
    FILE_FORMAT = JsonFileFormat
);
GO
```

**4. Query the Logs:**
```sql
-- Get all logs from today
SELECT
    timestamp,
    level,
    message,
    JSON_VALUE(extra, '$.user_identity') AS user,
    JSON_VALUE(extra, '$.client_ip') AS ip,
    JSON_VALUE(extra, '$.http_status') AS status,
    JSON_VALUE(extra, '$.response_time_ms') AS response_time
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY timestamp DESC;

-- Aggregate by user
SELECT
    JSON_VALUE(extra, '$.user_identity') AS user,
    COUNT(*) AS total_requests,
    AVG(CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS avg_response_time,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS errors
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -7, GETDATE())
GROUP BY JSON_VALUE(extra, '$.user_identity')
ORDER BY total_requests DESC;

-- Find slow requests
SELECT
    timestamp,
    JSON_VALUE(extra, '$.user_identity') AS user,
    JSON_VALUE(extra, '$.request_path') AS endpoint,
    JSON_VALUE(extra, '$.response_time_ms') AS response_time_ms
FROM ApiRequestLogs
WHERE CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT) > 1000  -- Slower than 1 second
ORDER BY CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT) DESC;
```

### Option B: Databricks (Delta Lake)

**1. Mount Storage Account:**
```python
# Configure storage account access
storage_account = "deltasharelogsstorage"
container = "deltashare-logs"

# Using account key
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope="your-scope", key="storage-account-key")
)

# Or using managed identity (recommended)
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.blob.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.blob.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
)
```

**2. Read JSON Logs:**
```python
# Read all logs
logs_df = (
    spark.read
    .format("json")
    .load(f"wasbs://{container}@{storage_account}.blob.core.windows.net/")
)

# Display schema
logs_df.printSchema()

# Show sample
logs_df.show(10, truncate=False)
```

**3. Create Delta Table:**
```python
from pyspark.sql.functions import col, from_json, get_json_object

# Extract fields from nested JSON
processed_df = (
    logs_df
    .withColumn("request_id", get_json_object(col("extra"), "$.request_id"))
    .withColumn("user_identity", get_json_object(col("extra"), "$.user_identity"))
    .withColumn("client_ip", get_json_object(col("extra"), "$.client_ip"))
    .withColumn("http_method", get_json_object(col("extra"), "$.http_method"))
    .withColumn("url_path", get_json_object(col("extra"), "$.url_path"))
    .withColumn("http_status", get_json_object(col("extra"), "$.http_status").cast("int"))
    .withColumn("response_time_ms", get_json_object(col("extra"), "$.response_time_ms").cast("double"))
    .withColumn("event_type", get_json_object(col("extra"), "$.event_type"))
)

# Write to Delta table (partitioned by date)
(
    processed_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("timestamp")  # Or extract date from timestamp
    .save("/mnt/delta/api_logs")
)

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS api_request_logs
    USING DELTA
    LOCATION '/mnt/delta/api_logs'
""")
```

**4. Query Delta Table:**
```sql
-- Using SQL
SELECT
    timestamp,
    user_identity,
    http_method,
    url_path,
    http_status,
    response_time_ms
FROM api_request_logs
WHERE DATE(timestamp) = CURRENT_DATE()
ORDER BY timestamp DESC;

-- Aggregate statistics
SELECT
    user_identity,
    COUNT(*) as total_requests,
    AVG(response_time_ms) as avg_response_time,
    COUNT(CASE WHEN http_status >= 400 THEN 1 END) as errors
FROM api_request_logs
WHERE DATE(timestamp) >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY user_identity
ORDER BY total_requests DESC;
```

### Option C: Azure Data Explorer (Kusto)

**1. Create External Table:**
```kusto
// Create external table pointing to blob storage
.create external table ApiLogs (
    timestamp: datetime,
    level: string,
    logger: string,
    message: string,
    extra: dynamic  // JSON object
)
kind=blob
dataformat=json
(
    h@'https://deltasharelogsstorage.blob.core.windows.net/deltashare-logs;managed_identity=system'
)
with (folder = '/', includeHeaders='firstFile')
```

**2. Query Logs:**
```kusto
// Get recent requests
ApiLogs
| where timestamp > ago(1h)
| extend user = tostring(extra.user_identity)
| extend ip = tostring(extra.client_ip)
| extend status = toint(extra.http_status)
| extend response_time = toreal(extra.response_time_ms)
| project timestamp, user, ip, status, response_time, message
| order by timestamp desc
| take 100

// Aggregate by user
ApiLogs
| where timestamp > ago(7d)
| extend user = tostring(extra.user_identity)
| extend response_time = toreal(extra.response_time_ms)
| extend is_error = level in ("ERROR", "CRITICAL")
| summarize
    requests = count(),
    avg_response_time = avg(response_time),
    errors = countif(is_error)
    by user
| order by requests desc
```

---

## 🔍 Step 5: Analytics Examples

### Request Volume by Hour
```sql
-- Synapse SQL
SELECT
    DATEPART(HOUR, CAST(timestamp AS DATETIME)) AS hour,
    COUNT(*) AS request_count
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY DATEPART(HOUR, CAST(timestamp AS DATETIME))
ORDER BY hour;
```

### Top Users by Request Count
```sql
SELECT TOP 10
    JSON_VALUE(extra, '$.user_identity') AS user,
    COUNT(*) AS total_requests,
    COUNT(DISTINCT JSON_VALUE(extra, '$.url_path')) AS unique_endpoints,
    AVG(CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS avg_response_time
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -30, GETDATE())
GROUP BY JSON_VALUE(extra, '$.user_identity')
ORDER BY total_requests DESC;
```

### Error Rate by Endpoint
```sql
SELECT
    JSON_VALUE(extra, '$.url_path') AS endpoint,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN level IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
    CAST(SUM(CASE WHEN level IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS error_rate_pct
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -7, GETDATE())
GROUP BY JSON_VALUE(extra, '$.url_path')
HAVING COUNT(*) > 10  -- Only endpoints with significant traffic
ORDER BY error_rate_pct DESC;
```

### Response Time Percentiles
```sql
SELECT
    JSON_VALUE(extra, '$.url_path') AS endpoint,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS p50,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS p99
FROM ApiRequestLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -7, GETDATE())
  AND JSON_VALUE(extra, '$.response_time_ms') IS NOT NULL
GROUP BY JSON_VALUE(extra, '$.url_path')
ORDER BY p99 DESC;
```

---

## 💰 Cost Optimization

### 1. Lifecycle Management

Set up automatic archival/deletion of old logs:

```bash
# Create lifecycle management policy (via Azure Portal or CLI)
az storage account management-policy create \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --policy @policy.json
```

**policy.json:**
```json
{
  "rules": [
    {
      "enabled": true,
      "name": "archiveOldLogs",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "baseBlob": {
            "tierToCool": {
              "daysAfterModificationGreaterThan": 30
            },
            "tierToArchive": {
              "daysAfterModificationGreaterThan": 90
            },
            "delete": {
              "daysAfterModificationGreaterThan": 365
            }
          }
        },
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["deltashare-logs/"]
        }
      }
    }
  ]
}
```

**Policy:**
- ✅ After 30 days: Move to Cool tier (lower storage cost)
- ✅ After 90 days: Move to Archive tier (lowest cost)
- ✅ After 365 days: Delete (compliance retention)

### 2. Query Optimization

Only query the partitions you need:

```sql
-- Good: Query specific date range
SELECT * FROM ApiRequestLogs
WHERE timestamp >= '2026/01/05'
  AND timestamp < '2026/01/06';

-- Bad: Full table scan
SELECT * FROM ApiRequestLogs;
```

---

## ✅ Step 6: Verify It's Working

### 1. Restart Web App
```bash
az webapp restart \
  --name webagenticops \
  --resource-group your-resource-group
```

### 2. Make Test Requests
```bash
curl https://webagenticops.azurewebsites.net/health
curl https://webagenticops.azurewebsites.net/shares
```

### 3. Check Blob Storage

**Via Azure Portal:**
1. Go to Storage Account → Containers → deltashare-logs
2. Navigate into the date folders (2026/01/05/...)
3. Download and view a JSON file

**Via Azure CLI:**
```bash
# List recent logs
az storage blob list \
  --container-name deltashare-logs \
  --account-name $STORAGE_ACCOUNT \
  --prefix "2026/01/05/" \
  --auth-mode login \
  --output table

# Download a log file
az storage blob download \
  --container-name deltashare-logs \
  --account-name $STORAGE_ACCOUNT \
  --name "2026/01/05/14/log_20260105_140532_123456.json" \
  --file ./sample-log.json \
  --auth-mode login

# View the log
cat sample-log.json | jq .
```

### 4. Expected JSON Structure

You should see logs like this:

```json
{
  "timestamp": "2026-01-05T14:05:32.123456Z",
  "level": "INFO",
  "logger": "dbrx_api.monitoring.request_context",
  "function": "dispatch",
  "line": 88,
  "message": "Request completed",
  "extra": {
    "request_id": "abc-123",
    "client_ip": "20.185.45.67",
    "user_identity": "john.doe@company.com",
    "user_agent": "curl/7.68.0",
    "request_path": "GET /health",
    "http_method": "GET",
    "url_path": "/health",
    "http_status": 200,
    "response_time_ms": 12.34,
    "event_type": "request_completed"
  }
}
```

✅ **Success indicators:**
- Files are being created in blob storage
- Files are organized by date/time
- JSON contains request/response details
- User identity and IP are captured

---

## 🔒 Security Best Practices

### 1. Use Managed Identity (Not Keys)
```bash
# Enable managed identity
az webapp identity assign --name $WEB_APP_NAME --resource-group $RESOURCE_GROUP

# Grant storage access
az role assignment create \
  --assignee $(az webapp identity show --name $WEB_APP_NAME --resource-group $RESOURCE_GROUP --query principalId -o tsv) \
  --role "Storage Blob Data Contributor" \
  --scope $(az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query id -o tsv)
```

### 2. Disable Public Access
```bash
az storage account update \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --allow-blob-public-access false
```

### 3. Use Private Endpoints (Optional)
For maximum security, use VNet integration with Private Link.

### 4. Encrypt at Rest
Azure Storage automatically encrypts all data. For extra security, use customer-managed keys.

---

## 🎯 Summary

You've set up:
- ✅ Azure Blob Storage for structured logging
- ✅ Automatic partitioning by date/time
- ✅ Request/response tracking with full context
- ✅ External table support for analytics
- ✅ Cost-optimized lifecycle policies
- ✅ Secure access with managed identity

Your logs are now queryable with:
- ✅ Azure Synapse Analytics (SQL)
- ✅ Databricks (Delta Lake)
- ✅ Azure Data Explorer (Kusto)
- ✅ Any tool that reads JSON from blob storage

**Next steps:**
1. Create storage account
2. Configure environment variables
3. Deploy code (already done via git push)
4. Verify logs are being written
5. Set up external tables for analytics

See **QUICK_START_BLOB_LOGGING.md** for a condensed version!
