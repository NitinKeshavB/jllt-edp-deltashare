# Quick Start: Azure Blob Storage Logging

**5-minute setup for structured logging to Azure Blob Storage - perfect for external tables and analytics**

## 🎯 What You'll Get

All API requests/responses logged to blob storage in structured JSON format:
- ✅ Partitioned by date/time (YYYY/MM/DD/HH)
- ✅ Request/response details
- ✅ User identity and client IP
- ✅ Response times and status codes
- ✅ Ready for external tables (Synapse, Databricks, Kusto)

---

## ⚡ Quick Setup (3 Steps)

### Step 1: Create Storage Account (2 minutes)

**Azure CLI (fastest):**
```bash
# Replace with your values
RESOURCE_GROUP="your-resource-group"
STORAGE_ACCOUNT="deltasharelogsstorage"  # Must be globally unique

# Create storage account
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access false

# Create container
az storage container create \
  --name deltashare-logs \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login

# Get storage URL
STORAGE_URL=$(az storage account show \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query primaryEndpoints.blob \
  --output tsv)

echo "Storage URL: $STORAGE_URL"
```

### Step 2: Enable Managed Identity & Grant Access (1 minute)

```bash
WEB_APP_NAME="webagenticops"

# Enable managed identity on Web App
az webapp identity assign \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP

# Grant storage access to Web App
PRINCIPAL_ID=$(az webapp identity show \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --query principalId \
  --output tsv)

STORAGE_ID=$(az storage account show \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query id \
  --output tsv)

az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope $STORAGE_ID
```

### Step 3: Configure Environment Variables (1 minute)

```bash
az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    ENABLE_BLOB_LOGGING=true \
    AZURE_STORAGE_ACCOUNT_URL="$STORAGE_URL" \
    AZURE_STORAGE_LOGS_CONTAINER="deltashare-logs"
```

**Restart Web App:**
```bash
az webapp restart --name $WEB_APP_NAME --resource-group $RESOURCE_GROUP
```

---

## ✅ Verify It's Working

### Make Test Request
```bash
curl https://webagenticops.azurewebsites.net/health
```

### Check Logs (Azure Portal)
1. Go to Storage Account → Containers → `deltashare-logs`
2. Navigate into date folders: `2026/01/05/14/`
3. Download and view a JSON file

### Check Logs (Azure CLI)
```bash
# List today's logs
az storage blob list \
  --container-name deltashare-logs \
  --account-name $STORAGE_ACCOUNT \
  --prefix "$(date +%Y/%m/%d)/" \
  --auth-mode login \
  --output table

# Download a sample log
az storage blob download \
  --container-name deltashare-logs \
  --account-name $STORAGE_ACCOUNT \
  --name "2026/01/05/14/log_20260105_140532_123456.json" \
  --file ./sample.json \
  --auth-mode login

# Pretty print
cat sample.json | jq .
```

---

## 📊 Log Structure

Each log file contains:

```json
{
  "timestamp": "2026-01-05T14:05:32.123456Z",
  "level": "INFO",
  "logger": "dbrx_api.monitoring.request_context",
  "message": "Request completed",
  "extra": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "user_identity": "john.doe@company.com",
    "client_ip": "20.185.123.45",
    "http_method": "GET",
    "url_path": "/shares/my_share",
    "url_query": "limit=100",
    "http_status": 200,
    "response_time_ms": 45.23,
    "event_type": "request_completed"
  }
}
```

**Partitioning:** `YYYY/MM/DD/HH/log_YYYYMMDD_HHMMss_microseconds.json`

---

## 🔍 Query with External Tables

### Option 1: Azure Synapse (Serverless SQL)

```sql
-- Create external data source
CREATE EXTERNAL DATA SOURCE DeltaShareLogs
WITH (
    LOCATION = 'https://deltasharelogsstorage.blob.core.windows.net/deltashare-logs',
    CREDENTIAL = [Managed Identity]
);

-- Create external table
CREATE EXTERNAL TABLE ApiLogs
(
    timestamp VARCHAR(50),
    level VARCHAR(20),
    message VARCHAR(MAX),
    extra NVARCHAR(MAX)  -- JSON
)
WITH (
    LOCATION = '/',
    DATA_SOURCE = DeltaShareLogs,
    FILE_FORMAT = [JsonFormat]
);

-- Query
SELECT
    timestamp,
    JSON_VALUE(extra, '$.user_identity') AS user,
    JSON_VALUE(extra, '$.http_status') AS status,
    JSON_VALUE(extra, '$.response_time_ms') AS response_time
FROM ApiLogs
WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY timestamp DESC;
```

### Option 2: Databricks (Delta Lake)

```python
# Read logs
logs_df = (
    spark.read
    .format("json")
    .load("wasbs://deltashare-logs@deltasharelogsstorage.blob.core.windows.net/")
)

# Extract fields
from pyspark.sql.functions import get_json_object, col

processed_df = (
    logs_df
    .withColumn("user", get_json_object(col("extra"), "$.user_identity"))
    .withColumn("ip", get_json_object(col("extra"), "$.client_ip"))
    .withColumn("status", get_json_object(col("extra"), "$.http_status").cast("int"))
    .withColumn("response_time", get_json_object(col("extra"), "$.response_time_ms").cast("double"))
)

# Save as Delta table
processed_df.write.format("delta").save("/mnt/delta/api_logs")

# Query
spark.sql("""
    SELECT timestamp, user, status, response_time
    FROM delta.`/mnt/delta/api_logs`
    WHERE DATE(timestamp) = CURRENT_DATE()
    ORDER BY timestamp DESC
""")
```

### Option 3: Azure Data Explorer (Kusto)

```kusto
// Create external table
.create external table ApiLogs (
    timestamp: datetime,
    level: string,
    message: string,
    extra: dynamic
)
kind=blob
dataformat=json
(
    h@'https://deltasharelogsstorage.blob.core.windows.net/deltashare-logs;managed_identity=system'
)

// Query
ApiLogs
| where timestamp > ago(1d)
| extend user = tostring(extra.user_identity)
| extend status = toint(extra.http_status)
| extend response_time = toreal(extra.response_time_ms)
| project timestamp, user, status, response_time
| order by timestamp desc
```

---

## 💰 Cost Optimization

### Set Up Lifecycle Policy

**policy.json:**
```json
{
  "rules": [{
    "enabled": true,
    "name": "archiveOldLogs",
    "type": "Lifecycle",
    "definition": {
      "actions": {
        "baseBlob": {
          "tierToCool": {"daysAfterModificationGreaterThan": 30},
          "tierToArchive": {"daysAfterModificationGreaterThan": 90},
          "delete": {"daysAfterModificationGreaterThan": 365}
        }
      },
      "filters": {
        "blobTypes": ["blockBlob"],
        "prefixMatch": ["deltashare-logs/"]
      }
    }
  }]
}
```

**Apply policy:**
```bash
az storage account management-policy create \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --policy @policy.json
```

**Savings:**
- ✅ Cool tier (after 30 days): 50% storage cost reduction
- ✅ Archive tier (after 90 days): 90% storage cost reduction
- ✅ Auto-delete (after 365 days): 100% storage cost reduction

---

## 📈 Common Analytics Queries

### Request Volume by Hour
```sql
SELECT
    DATEPART(HOUR, CAST(timestamp AS DATETIME)) AS hour,
    COUNT(*) AS requests
FROM ApiLogs
WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY DATEPART(HOUR, CAST(timestamp AS DATETIME))
ORDER BY hour;
```

### Top Users
```sql
SELECT TOP 10
    JSON_VALUE(extra, '$.user_identity') AS user,
    COUNT(*) AS requests,
    AVG(CAST(JSON_VALUE(extra, '$.response_time_ms') AS FLOAT)) AS avg_ms
FROM ApiLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -7, GETDATE())
GROUP BY JSON_VALUE(extra, '$.user_identity')
ORDER BY requests DESC;
```

### Error Rate by Endpoint
```sql
SELECT
    JSON_VALUE(extra, '$.url_path') AS endpoint,
    COUNT(*) AS total,
    SUM(CASE WHEN level IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
    CAST(SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS error_pct
FROM ApiLogs
WHERE CAST(timestamp AS DATE) >= DATEADD(day, -7, GETDATE())
GROUP BY JSON_VALUE(extra, '$.url_path')
ORDER BY error_pct DESC;
```

---

## ✅ Checklist

- [ ] Storage account created
- [ ] Container `deltashare-logs` created
- [ ] Managed identity enabled on Web App
- [ ] Storage access granted to Web App
- [ ] Environment variables configured
- [ ] Web App restarted
- [ ] Test request made
- [ ] Logs visible in blob storage
- [ ] Lifecycle policy configured (optional)

**Done! Your API logs are now in blob storage with structured format for analytics.**

---

## 📚 Full Documentation

See [AZURE_BLOB_LOGGING_GUIDE.md](./AZURE_BLOB_LOGGING_GUIDE.md) for:
- Detailed setup instructions
- External table examples (Synapse, Databricks, Kusto)
- Advanced analytics queries
- Security best practices
- Troubleshooting guide
