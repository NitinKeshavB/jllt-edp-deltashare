# Quick Start: Enable Database Logging

**5-minute setup to enable request tracking in your Azure Web App**

## 🎯 What You'll Get

After this setup, every API request will be logged to PostgreSQL with:
- ✅ **Who**: User identity (Azure AD, tokens, API keys)
- ✅ **Where**: Client IP, origin, referer
- ✅ **When**: Timestamp
- ✅ **What**: Request path, method, parameters
- ✅ **Outcome**: Response status, errors, exceptions

---

## ⚡ Quick Setup (3 Steps)

### Step 1: Create PostgreSQL Database (5 minutes)

**Option A: Azure Portal**
1. Go to [Azure Portal](https://portal.azure.com) → Create Resource
2. Search "Azure Database for PostgreSQL Flexible Server"
3. Create with these settings:
   - **Name**: `deltashare-logs-db`
   - **Region**: Same as your Web App
   - **Tier**: Burstable B1ms (cost-effective)
   - **Admin username**: `pgadmin`
   - **Password**: [Choose strong password]
4. **Networking**: Allow Azure services (0.0.0.0)
5. Create!

**Option B: Azure CLI (faster)**
```bash
# Replace these values
RESOURCE_GROUP="your-resource-group"
SERVER_NAME="deltashare-logs-db"
ADMIN_PASSWORD="YourSecurePassword123!"

az postgres flexible-server create \
  --resource-group $RESOURCE_GROUP \
  --name $SERVER_NAME \
  --admin-user pgadmin \
  --admin-password $ADMIN_PASSWORD \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --public-access 0.0.0.0

# Create database
az postgres flexible-server db create \
  --resource-group $RESOURCE_GROUP \
  --server-name $SERVER_NAME \
  --database-name deltashare_logs
```

### Step 2: Add Environment Variables (2 minutes)

**Via Azure Portal:**
1. Go to your Web App → **Configuration** → **Application settings**
2. Add these settings:

```
ENABLE_POSTGRESQL_LOGGING = true

POSTGRESQL_CONNECTION_STRING = postgresql://pgadmin:YourPassword@deltashare-logs-db.postgres.database.azure.com:5432/deltashare_logs?sslmode=require

POSTGRESQL_LOG_TABLE = api_request_logs

POSTGRESQL_MIN_LOG_LEVEL = INFO
```

3. Click **Save** → **Continue**

**Via Azure CLI:**
```bash
WEB_APP_NAME="webagenticops"
RESOURCE_GROUP="your-resource-group"

az webapp config appsettings set \
  --name $WEB_APP_NAME \
  --resource-group $RESOURCE_GROUP \
  --settings \
    ENABLE_POSTGRESQL_LOGGING=true \
    POSTGRESQL_CONNECTION_STRING="postgresql://pgadmin:YourPassword@deltashare-logs-db.postgres.database.azure.com:5432/deltashare_logs?sslmode=require" \
    POSTGRESQL_LOG_TABLE="api_request_logs" \
    POSTGRESQL_MIN_LOG_LEVEL="INFO"
```

### Step 3: Deploy & Verify (1 minute)

**Deploy the updated code:**
```bash
git add .
git commit -m "feat: add database logging with request tracking"
git push
```

**Test it:**
```bash
# Make a request
curl https://webagenticops.azurewebsites.net/health

# Check database (via Azure Portal Query Editor or psql)
SELECT
    timestamp,
    level,
    message,
    extra_data->>'user_identity' AS user,
    extra_data->>'client_ip' AS ip,
    extra_data->>'request_path' AS request
FROM api_request_logs
ORDER BY timestamp DESC
LIMIT 10;
```

✅ **You should see logs with user and IP information!**

---

## 📊 Example Queries

### Who is using the API?

```sql
SELECT
    extra_data->>'user_identity' AS user,
    COUNT(*) AS requests,
    MAX(timestamp) AS last_seen
FROM api_request_logs
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY extra_data->>'user_identity'
ORDER BY requests DESC;
```

### Requests from specific user

```sql
SELECT
    timestamp,
    extra_data->>'request_path' AS endpoint,
    extra_data->>'client_ip' AS ip,
    level,
    message
FROM api_request_logs
WHERE extra_data->>'user_identity' LIKE '%john.doe%'
ORDER BY timestamp DESC
LIMIT 50;
```

### Track errors by user

```sql
SELECT
    extra_data->>'user_identity' AS user,
    extra_data->>'request_path' AS endpoint,
    COUNT(*) AS error_count
FROM api_request_logs
WHERE level IN ('ERROR', 'CRITICAL')
  AND timestamp > NOW() - INTERVAL '7 days'
GROUP BY
    extra_data->>'user_identity',
    extra_data->>'request_path'
ORDER BY error_count DESC;
```

### API usage by source

```sql
SELECT
    extra_data->>'origin' AS source,
    COUNT(*) AS requests,
    COUNT(DISTINCT extra_data->>'user_identity') AS unique_users
FROM api_request_logs
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY extra_data->>'origin'
ORDER BY requests DESC;
```

---

## 🔒 Security Tips

1. **Use Key Vault** (recommended):
   ```bash
   # Store connection string in Key Vault
   az keyvault secret set \
     --vault-name your-keyvault \
     --name postgresql-conn \
     --value "postgresql://..."

   # Reference in Web App
   POSTGRESQL_CONNECTION_STRING="@Microsoft.KeyVault(SecretUri=https://your-keyvault.vault.azure.net/secrets/postgresql-conn/)"
   ```

2. **Restrict Database Access**:
   - Only allow Web App's outbound IPs
   - Or use VNet integration + Private Link

3. **Create Dedicated User**:
   ```sql
   CREATE USER logger WITH PASSWORD 'SecurePass';
   GRANT INSERT, SELECT ON api_request_logs TO logger;
   ```

---

## 🎓 User Identity Detection

The middleware automatically detects users from:

### 1. Azure AD (Automatic with Easy Auth)
Enable in: Web App → Authentication → Add Microsoft identity

Logs as: `john.doe@company.com (user-id)`

### 2. Bearer Tokens (JWT)
Header: `Authorization: Bearer token...`

Logs as: `bearer_token:eyJhbG...`

### 3. API Keys
Header: `X-API-Key: your-key`

Logs as: `api_key:your-key...`

### 4. Anonymous
No authentication detected

Logs as: `anonymous`

---

## 📚 Need More Details?

See the complete guide: [DATABASE_LOGGING_GUIDE.md](./DATABASE_LOGGING_GUIDE.md)

Topics covered:
- Advanced queries and analytics
- Dashboard creation
- Performance optimization
- Troubleshooting
- Compliance and retention policies

---

## ✅ Checklist

- [ ] PostgreSQL database created
- [ ] Environment variables configured in Web App
- [ ] Code deployed to Azure
- [ ] Test request made
- [ ] Logs visible in database
- [ ] Queries returning expected data

**Done! Your API now tracks every request with full context.**
