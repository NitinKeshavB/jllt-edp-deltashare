# Database Logging Setup Guide

Complete guide to enable PostgreSQL database logging for the Delta Share API with request tracking (who/where).

## 🎯 What Gets Logged

The database logging captures:

### Request Context
- ✅ **Who**: User identity from Azure AD, Bearer tokens, API keys, or certificates
- ✅ **Where**: Client IP address, origin, referer
- ✅ **What**: Request path, method, query parameters
- ✅ **When**: Timestamp with timezone
- ✅ **How**: User agent (browser/client info)

### Application Logs
- ✅ **Level**: INFO, WARNING, ERROR, CRITICAL
- ✅ **Message**: Log message with structured data
- ✅ **Extra Data**: Custom fields (JSONB for querying)
- ✅ **Exceptions**: Full stack traces for errors
- ✅ **Function**: Code location (function, line number)

---

## 📋 Prerequisites

1. **Azure Database for PostgreSQL** (Flexible Server recommended)
2. **Network Access** from Azure Web App to PostgreSQL
3. **Database Credentials** with CREATE TABLE permissions

---

## 🚀 Step 1: Create Azure Database for PostgreSQL

### Option A: Azure Portal

1. Go to [Azure Portal](https://portal.azure.com)
2. Create new **Azure Database for PostgreSQL Flexible Server**
3. Configuration:
   - **Server name**: `deltashare-logs-db` (or your choice)
   - **Region**: Same as your Web App
   - **PostgreSQL version**: 14 or higher
   - **Compute + Storage**:
     - Basic: 1 vCore, 32 GB storage (low cost)
     - Standard: 2 vCores, 64 GB storage (production)
   - **Authentication**: PostgreSQL authentication
   - **Admin username**: `pgadmin` (or your choice)
   - **Password**: Strong password (save securely!)

4. **Networking**:
   - Enable "Allow public access from Azure services"
   - Add your Web App's outbound IPs to firewall rules
   - Or use VNet integration for better security

5. **Review + Create**

### Option B: Azure CLI

```bash
# Variables
RESOURCE_GROUP="your-resource-group"
LOCATION="eastus"
SERVER_NAME="deltashare-logs-db"
ADMIN_USER="pgadmin"
ADMIN_PASSWORD="YourSecurePassword123!"  # Change this!
DATABASE_NAME="deltashare_logs"

# Create PostgreSQL server
az postgres flexible-server create \
  --resource-group $RESOURCE_GROUP \
  --name $SERVER_NAME \
  --location $LOCATION \
  --admin-user $ADMIN_USER \
  --admin-password $ADMIN_PASSWORD \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --storage-size 32 \
  --version 14 \
  --public-access 0.0.0.0

# Create database
az postgres flexible-server db create \
  --resource-group $RESOURCE_GROUP \
  --server-name $SERVER_NAME \
  --database-name $DATABASE_NAME

# Allow Azure services
az postgres flexible-server firewall-rule create \
  --resource-group $RESOURCE_GROUP \
  --name $SERVER_NAME \
  --rule-name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0
```

---

## 🔧 Step 2: Configure Environment Variables

Add these environment variables to your Azure Web App:

### Required Variables

```bash
# Enable PostgreSQL logging
ENABLE_POSTGRESQL_LOGGING=true

# PostgreSQL connection string
# Format: postgresql://username:password@hostname:port/database?sslmode=require
POSTGRESQL_CONNECTION_STRING="postgresql://pgadmin:YourPassword@deltashare-logs-db.postgres.database.azure.com:5432/deltashare_logs?sslmode=require"

# Optional: Customize table name (default: application_logs)
POSTGRESQL_LOG_TABLE="api_request_logs"

# Optional: Minimum log level to store (default: WARNING)
# Options: TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL
POSTGRESQL_MIN_LOG_LEVEL="INFO"
```

### Setting Variables in Azure Portal

1. Navigate to your Web App → **Configuration** → **Application settings**
2. Click **+ New application setting** for each variable
3. Add the settings above
4. Click **Save** and **Continue** when prompted to restart

### Setting Variables with Azure CLI

```bash
WEB_APP_NAME="webagenticops"  # Your web app name
RESOURCE_GROUP="your-resource-group"

# Set PostgreSQL logging environment variables
az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    ENABLE_POSTGRESQL_LOGGING=true \
    POSTGRESQL_CONNECTION_STRING="postgresql://pgadmin:YourPassword@deltashare-logs-db.postgres.database.azure.com:5432/deltashare_logs?sslmode=require" \
    POSTGRESQL_LOG_TABLE="api_request_logs" \
    POSTGRESQL_MIN_LOG_LEVEL="INFO"
```

---

## 📊 Step 3: Database Schema

The application **automatically creates** the required table on first startup. The schema includes:

```sql
CREATE TABLE IF NOT EXISTS application_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    level VARCHAR(20) NOT NULL,
    logger_name VARCHAR(255),
    function_name VARCHAR(255),
    line_number INTEGER,
    message TEXT NOT NULL,
    extra_data JSONB,  -- Includes request_id, client_ip, user_identity, etc.
    exception_type VARCHAR(255),
    exception_value TEXT,
    exception_traceback TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_application_logs_timestamp ON application_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_application_logs_level ON application_logs(level);
CREATE INDEX IF NOT EXISTS idx_application_logs_created_at ON application_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_application_logs_extra_data ON application_logs USING GIN(extra_data);
```

### Extra Data JSONB Fields

The `extra_data` column stores request context as JSON:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "client_ip": "20.185.123.45",
  "user_identity": "john.doe@company.com (abc123)",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
  "request_path": "GET /shares/my_share",
  "referer": "https://example.com/dashboard",
  "origin": "https://example.com"
}
```

---

## 🔍 Step 4: Querying Logs

### Connect to Database

```bash
# Using psql
psql "postgresql://pgadmin:YourPassword@deltashare-logs-db.postgres.database.azure.com:5432/deltashare_logs?sslmode=require"

# Or using Azure CLI
az postgres flexible-server connect \
  --name deltashare-logs-db \
  --admin-user pgadmin \
  --database-name deltashare_logs
```

### Example Queries

#### 1. Recent Requests by User

```sql
-- Get all requests from a specific user in the last 24 hours
SELECT
    timestamp,
    level,
    message,
    extra_data->>'user_identity' AS user,
    extra_data->>'client_ip' AS ip,
    extra_data->>'request_path' AS request
FROM application_logs
WHERE extra_data->>'user_identity' LIKE '%john.doe%'
  AND timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC
LIMIT 100;
```

#### 2. Requests from Specific IP Address

```sql
-- Track all requests from a specific IP
SELECT
    timestamp,
    extra_data->>'user_identity' AS user,
    extra_data->>'request_path' AS request,
    level,
    message
FROM application_logs
WHERE extra_data->>'client_ip' = '20.185.123.45'
ORDER BY timestamp DESC;
```

#### 3. Error Logs with User Context

```sql
-- Get all errors with who triggered them
SELECT
    timestamp,
    level,
    message,
    extra_data->>'user_identity' AS user,
    extra_data->>'client_ip' AS ip,
    extra_data->>'request_path' AS request,
    exception_type,
    exception_value
FROM application_logs
WHERE level IN ('ERROR', 'CRITICAL')
  AND timestamp > NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;
```

#### 4. Request Activity by User

```sql
-- Count requests per user in the last 24 hours
SELECT
    extra_data->>'user_identity' AS user,
    COUNT(*) AS request_count,
    MIN(timestamp) AS first_request,
    MAX(timestamp) AS last_request
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '24 hours'
  AND extra_data ? 'user_identity'
GROUP BY extra_data->>'user_identity'
ORDER BY request_count DESC;
```

#### 5. Requests by Source (Referer/Origin)

```sql
-- Track where requests are coming from
SELECT
    extra_data->>'origin' AS source,
    COUNT(*) AS request_count,
    COUNT(DISTINCT extra_data->>'user_identity') AS unique_users
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '7 days'
  AND extra_data ? 'origin'
GROUP BY extra_data->>'origin'
ORDER BY request_count DESC;
```

#### 6. API Usage by Endpoint

```sql
-- Most used API endpoints
SELECT
    extra_data->>'request_path' AS endpoint,
    COUNT(*) AS hit_count,
    COUNT(DISTINCT extra_data->>'user_identity') AS unique_users,
    COUNT(CASE WHEN level IN ('ERROR', 'CRITICAL') THEN 1 END) AS errors
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '7 days'
  AND extra_data ? 'request_path'
GROUP BY extra_data->>'request_path'
ORDER BY hit_count DESC;
```

#### 7. Suspicious Activity Detection

```sql
-- Detect potential suspicious activity (many requests from anonymous users)
SELECT
    extra_data->>'client_ip' AS ip,
    extra_data->>'user_identity' AS user,
    COUNT(*) AS request_count,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '1 hour'
  AND extra_data->>'user_identity' = 'anonymous'
GROUP BY extra_data->>'client_ip', extra_data->>'user_identity'
HAVING COUNT(*) > 100  -- More than 100 requests in 1 hour
ORDER BY request_count DESC;
```

---

## 📈 Step 5: Monitoring Dashboard (Optional)

### Create a View for Common Queries

```sql
-- Create a materialized view for faster dashboard queries
CREATE MATERIALIZED VIEW daily_api_stats AS
SELECT
    DATE(timestamp) AS date,
    extra_data->>'user_identity' AS user,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS errors,
    COUNT(CASE WHEN level = 'WARNING' THEN 1 END) AS warnings,
    COUNT(DISTINCT extra_data->>'client_ip') AS unique_ips,
    ARRAY_AGG(DISTINCT extra_data->>'request_path') FILTER (WHERE extra_data ? 'request_path') AS endpoints_used
FROM application_logs
WHERE timestamp > NOW() - INTERVAL '90 days'
GROUP BY DATE(timestamp), extra_data->>'user_identity';

-- Refresh view daily (or set up auto-refresh)
REFRESH MATERIALIZED VIEW daily_api_stats;
```

### Query the Dashboard View

```sql
-- Daily usage by user
SELECT * FROM daily_api_stats
WHERE date > CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC, total_requests DESC;
```

---

## 🔒 Security Best Practices

### 1. Secure Connection Strings

**DO NOT** commit connection strings to git. Always use environment variables.

### 2. Use Azure Key Vault (Recommended)

Store the connection string in Azure Key Vault:

```bash
# Store in Key Vault
az keyvault secret set \
  --vault-name your-keyvault \
  --name postgresql-connection-string \
  --value "postgresql://..."

# Reference in Web App
az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    POSTGRESQL_CONNECTION_STRING="@Microsoft.KeyVault(SecretUri=https://your-keyvault.vault.azure.net/secrets/postgresql-connection-string/)"
```

### 3. Network Security

- Use **VNet integration** to keep database private
- Enable **firewall rules** to allow only your Web App
- Use **Private Link** for maximum security

### 4. Database User Permissions

Create a dedicated user with minimal permissions:

```sql
-- Connect as admin
CREATE USER deltashare_logger WITH PASSWORD 'SecurePassword123!';

-- Grant only necessary permissions
GRANT CONNECT ON DATABASE deltashare_logs TO deltashare_logger;
GRANT USAGE ON SCHEMA public TO deltashare_logger;
GRANT INSERT, SELECT ON TABLE application_logs TO deltashare_logger;
GRANT USAGE, SELECT ON SEQUENCE application_logs_id_seq TO deltashare_logger;

-- Update connection string to use this user
-- postgresql://deltashare_logger:SecurePassword123!@...
```

### 5. Retention Policy

Set up automatic cleanup of old logs:

```sql
-- Create a function to delete logs older than 90 days
CREATE OR REPLACE FUNCTION cleanup_old_logs()
RETURNS void AS $$
BEGIN
    DELETE FROM application_logs
    WHERE timestamp < NOW() - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- Schedule with pg_cron (if available) or Azure Automation
-- Or run manually/via cron job
SELECT cleanup_old_logs();
```

---

## ✅ Step 6: Verify It's Working

### 1. Restart Your Web App

```bash
az webapp restart \
  --name webagenticops \
  --resource-group your-resource-group
```

### 2. Make a Test Request

```bash
# Make a request to your API
curl -X GET "https://webagenticops.azurewebsites.net/health" \
  -H "User-Agent: TestClient/1.0"
```

### 3. Check Database Logs

```sql
-- Check if logs are being written
SELECT
    timestamp,
    level,
    message,
    extra_data->>'request_id' AS request_id,
    extra_data->>'client_ip' AS ip,
    extra_data->>'user_identity' AS user
FROM application_logs
ORDER BY timestamp DESC
LIMIT 10;
```

You should see:
- ✅ Request logs with timestamps
- ✅ Client IP addresses
- ✅ User identity information
- ✅ Request paths and methods

---

## 🎯 User Identity Sources

The middleware automatically detects user identity from multiple sources:

### 1. Azure AD (Easy Auth)

When Azure App Service Authentication is enabled:
- Header: `X-MS-CLIENT-PRINCIPAL-NAME` (e.g., `john.doe@company.com`)
- Header: `X-MS-CLIENT-PRINCIPAL-ID` (e.g., `abc123-def456`)

**To enable Azure AD auth:**
1. Go to Web App → **Authentication**
2. Add identity provider → **Microsoft**
3. Configure Azure AD tenant

### 2. Bearer Tokens (JWT)

For API clients using JWT tokens:
- Header: `Authorization: Bearer eyJhbGci...`
- Logged as: `bearer_token:eyJhbGci...` (first 20 chars)

**To decode user from JWT:** Modify `request_context.py` to decode the token.

### 3. API Keys

For machine-to-machine authentication:
- Header: `X-API-Key: your-api-key`
- Logged as: `api_key:your-api-...` (preview)

**To track API key owner:** Maintain a mapping table in your database.

### 4. Client Certificates (mTLS)

For certificate-based authentication:
- Header: `X-ARR-ClientCert` (Azure App Service)
- Logged as: `mtls:certificate_auth`

### 5. Anonymous

If no authentication is detected:
- Logged as: `anonymous`

---

## 🔧 Troubleshooting

### Logs Not Appearing in Database

**Check 1: Environment Variables**
```bash
az webapp config appsettings list \
  --name webagenticops \
  --resource-group your-resource-group \
  | grep POSTGRESQL
```

**Check 2: Connection String Format**
Ensure SSL mode is enabled: `?sslmode=require`

**Check 3: Network Access**
Verify Web App can reach PostgreSQL:
```bash
# From Web App console (SSH)
psql "postgresql://..."
```

**Check 4: Application Logs**
```bash
az webapp log tail \
  --name webagenticops \
  --resource-group your-resource-group
```

Look for PostgreSQL connection errors.

### Performance Issues

**Solution 1: Add Indexes**
```sql
-- Add custom indexes for your queries
CREATE INDEX idx_user_timestamp ON application_logs(
    (extra_data->>'user_identity'),
    timestamp DESC
);
```

**Solution 2: Partition Table**
For high-volume logging, partition by date:
```sql
-- Convert to partitioned table (requires migration)
CREATE TABLE application_logs_2026_01 PARTITION OF application_logs
FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

**Solution 3: Adjust Buffer Size**
In settings.py, you can batch log writes (future enhancement).

---

## 📚 Summary

You've now set up:
- ✅ PostgreSQL database logging
- ✅ Request context tracking (who/where/when)
- ✅ User identity detection (Azure AD, JWT, API keys)
- ✅ Client IP tracking
- ✅ Structured logging with JSONB queries
- ✅ Security best practices

Your Delta Share API now tracks every request with full context!

---

## 📞 Support

For issues or questions:
- Check application logs: `az webapp log tail`
- Review PostgreSQL logs in Azure Portal
- Verify network connectivity and firewall rules
- Ensure environment variables are set correctly
