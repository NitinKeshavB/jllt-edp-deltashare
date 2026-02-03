# Workflow System - Next Steps Guide

## Status: Database Tables Created ✅

The workflow system is fully implemented with all 16 database tables in place. Here's your step-by-step guide to testing and deploying the system.

---

## Phase 1: Initial Testing (Today)

### Step 1: Configure Environment

Edit your `.env` file:

```env
# ============================================================
# Workflow Configuration
# ============================================================

# Enable workflow feature
enable_workflow=true

# Database connection (PostgreSQL 14+)
domain_db_connection_string=postgresql://username:password@hostname:5432/database_name

# Azure Storage Queue (get from Azure Portal > Storage Account > Access Keys)
azure_queue_connection_string=DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=abc123...==;EndpointSuffix=core.windows.net
azure_queue_name=sharepack-processing
```

### Step 2: Start Application

```bash
cd /home/nitinkeshav/JLLT-EDP-DELTASHARE/api_layer
make run-dev
```

**Expected logs:**
```
INFO: Workflow system enabled
INFO: Domain DB pool created successfully
SUCCESS: Workflow database initialized
SUCCESS: All 16 workflow tables verified successfully
INFO: Workflow queue client initialized
SUCCESS: Share pack queue consumer started
```

### Step 3: Test Health Endpoint

```bash
curl -X GET "http://localhost:8000/workflow/health" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

**Expected response:**
```json
{
  "Message": "Workflow system healthy",
  "DatabaseConnected": true,
  "QueueConnected": true,
  "TablesCount": 16
}
```

**If unhealthy:**
- Check `.env` configuration
- Verify database exists and is accessible
- Verify Azure Storage connection string is valid

### Step 4: Verify Database Tables

Connect to your PostgreSQL database:

```sql
-- Connect to database
psql -h hostname -U username -d database_name

-- Check schema exists
SELECT * FROM information_schema.schemata WHERE schema_name = 'deltashare';

-- List all tables (should show 16 tables)
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'deltashare'
ORDER BY table_name;

-- Expected tables:
/*
 ad_groups
 audit_trail
 databricks_objects
 job_metrics
 notifications
 pipelines
 project_costs
 projects
 recipients
 requests
 share_packs
 shares
 sync_jobs
 tenant_regions
 tenants
 users
*/
```

---

## Phase 2: Test File Upload (Today)

### Sample Files Created

Two sample files have been created in the `api_layer/` directory:

1. **sample_sharepack.yaml** - YAML format example
2. **sample_sharepack.xlsx** - Excel format example (4 sheets)

Both files contain the same test data:
- 2 recipients (1 D2O, 1 D2D)
- 2 shares with multiple data objects

### Test YAML Upload

```bash
cd /home/nitinkeshav/JLLT-EDP-DELTASHARE/api_layer

curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sample_sharepack.yaml"
```

**Expected response:**
```json
{
  "Message": "Share pack uploaded successfully and queued for provisioning",
  "SharePackId": "550e8400-e29b-41d4-a716-446655440000",
  "SharePackName": "SharePack_test.user@jll.com_20240130_143022",
  "Status": "IN_PROGRESS",
  "ValidationErrors": [],
  "ValidationWarnings": []
}
```

**Save the SharePackId** - you'll need it to check status!

### Test Excel Upload

```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sample_sharepack.xlsx"
```

Should get the same response format with a different SharePackId.

### Verify Upload in Database

```sql
-- Check share packs were created
SELECT
    share_pack_id,
    share_pack_name,
    share_pack_status,
    requested_by,
    strategy,
    effective_from
FROM deltashare.share_packs
WHERE is_current = true
ORDER BY effective_from DESC
LIMIT 5;

-- View the parsed configuration (stored as JSONB)
SELECT
    share_pack_name,
    jsonb_pretty(config) as parsed_config
FROM deltashare.share_packs
WHERE is_current = true
ORDER BY effective_from DESC
LIMIT 1;

-- Check recipients were extracted
SELECT
    config->'recipient' as recipients
FROM deltashare.share_packs
WHERE is_current = true
ORDER BY effective_from DESC
LIMIT 1;
```

---

## Phase 3: Monitor Queue Processing (Today)

### Check Azure Storage Queue

**Option 1: Azure Portal**
1. Go to your Storage Account
2. Navigate to **Queues** in left menu
3. Click **sharepack-processing**
4. Should see messages in the queue

**Option 2: Azure CLI**
```bash
# Check queue exists
az storage queue exists \
  --name sharepack-processing \
  --account-name <your-storage-account>

# Peek at messages (without removing them)
az storage message peek \
  --queue-name sharepack-processing \
  --account-name <your-storage-account> \
  --num-messages 5

# Get queue metadata
az storage queue metadata show \
  --name sharepack-processing \
  --account-name <your-storage-account>
```

### Watch Application Logs

The queue consumer runs in the background and processes messages. Watch for:

```
INFO: Queue consumer polling for messages...
INFO: Received 1 message(s) from queue
INFO: Processing share pack: 550e8400-e29b-41d4-a716-446655440000
INFO: Starting provisioning with NEW strategy
INFO: Step 1/8: Resolving tenant 'Test Business Line'
INFO: Step 2/8: Resolving project 'Test Project - Q1 2024'
INFO: Step 3/8: Creating recipients...
INFO: Creating D2O recipient: test-recipient-d2o
INFO: Creating D2D recipient: test-recipient-d2d
INFO: Step 4/8: Creating shares...
INFO: Creating share: test_share_q1
INFO: Step 5/8: Attaching data objects to shares...
SUCCESS: Share pack provisioned successfully
INFO: Message deleted from queue
```

**If processing fails**, check logs for error details:
```
ERROR: Failed to create recipient: [error details]
ERROR: Share pack provisioning failed: [error details]
```

### Check Status Endpoint

Poll the status endpoint to track progress:

```bash
# Replace {share_pack_id} with your actual ID
curl -X GET "http://localhost:8000/workflow/sharepack/{share_pack_id}" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

**Status progression:**
1. Initially: `"Status": "IN_PROGRESS"`
2. After processing: `"Status": "COMPLETED"` or `"Status": "FAILED"`

**Example completed response:**
```json
{
  "SharePackId": "550e8400-e29b-41d4-a716-446655440000",
  "SharePackName": "SharePack_test.user@jll.com_20240130_143022",
  "Status": "COMPLETED",
  "Strategy": "NEW",
  "ProvisioningStatus": "All resources provisioned successfully",
  "ErrorMessage": "",
  "RequestedBy": "test.user@jll.com",
  "CreatedAt": "2024-01-30T15:00:00Z",
  "LastUpdated": "2024-01-30T15:02:30Z"
}
```

### Check Database Updates

```sql
-- Check share pack status updates
SELECT
    share_pack_name,
    share_pack_status,
    provisioning_status,
    error_message,
    effective_from
FROM deltashare.share_packs
WHERE share_pack_id = '{your-share-pack-id}'
ORDER BY version DESC;

-- Check history (SCD Type 2 - all versions)
SELECT
    version,
    share_pack_status,
    is_current,
    effective_from,
    effective_to,
    created_by,
    change_reason
FROM deltashare.share_packs
WHERE share_pack_id = '{your-share-pack-id}'
ORDER BY version;
```

---

## Phase 4: Test Error Scenarios (This Week)

### Test 1: Invalid YAML Syntax

Create `invalid_syntax.yaml`:
```yaml
metadata:
  requestor: test@jll.com
  project_name: "Test"
  # Missing required fields
recipient
  - name: test
```

Upload and expect 400 Bad Request:
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@invalid_syntax.yaml"
```

### Test 2: Invalid File Type

Try uploading a PDF or text file:
```bash
echo "not a valid sharepack" > invalid.txt
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@invalid.txt"
```

Expect 400 Bad Request with error message.

### Test 3: Missing Required Fields

Create `missing_fields.yaml`:
```yaml
metadata:
  requestor: test@jll.com
  # Missing project_name, business_line, strategy

recipient:
  - name: test-recipient
    type: D2O
    # Missing email
```

Should fail validation.

### Test 4: Invalid Databricks Workspace

Use a non-existent workspace URL:
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://invalid-workspace.azuredatabricks.net" \
  -F "file=@sample_sharepack.yaml"
```

Should fail workspace URL validation.

---

## Phase 5: Test with Real Databricks Workspace (This Week)

### Prerequisites

1. **Valid Databricks workspace** with credentials configured
2. **Service principal** with permissions to:
   - Create recipients
   - Create shares
   - Add data objects to shares
3. **Real data objects** that exist in the workspace

### Update Sample Files

Modify `sample_sharepack.yaml` with real data:

```yaml
metadata:
  requestor: your.email@jll.com
  project_name: "Real Project Name"
  business_line: "Real Business Line"
  strategy: NEW

recipient:
  - name: real-external-partner
    type: D2O
    email: partner@external.com
    allowed_ips:
      - 203.0.113.0/24

  - name: real-internal-analyst
    type: D2D
    metastore_id: "aws:us-west-2:your-real-metastore-id"

share:
  - name: real_data_share
    comment: "Real data sharing"
    recipients:
      - real-external-partner
      - real-internal-analyst
    data_objects:
      - your_catalog.your_schema.your_table  # Must exist!
```

### Test Real Provisioning

```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://your-real-workspace.azuredatabricks.net" \
  -F "file=@sample_sharepack.yaml"
```

### Verify in Databricks

1. **Check recipients created:**
   - Go to Databricks workspace
   - Navigate to **Data** > **Delta Sharing** > **Shared by me** > **Recipients**
   - Should see `real-external-partner` and `real-internal-analyst`

2. **Check shares created:**
   - Navigate to **Data** > **Delta Sharing** > **Shared by me** > **Shares**
   - Should see `real_data_share`
   - Click on share to verify data objects attached

3. **Check recipient has access:**
   - Click on share
   - Verify recipients listed in **Shared with**

---

## Phase 6: Production Deployment (Next Week)

### Infrastructure Setup

#### 1. Azure PostgreSQL Database

```bash
# Create PostgreSQL flexible server
az postgres flexible-server create \
  --resource-group <your-rg> \
  --name deltashare-workflow-db \
  --location eastus \
  --admin-user dbadmin \
  --admin-password <secure-password> \
  --sku-name Standard_B2s \
  --tier Burstable \
  --version 14 \
  --storage-size 32

# Create database
az postgres flexible-server db create \
  --resource-group <your-rg> \
  --server-name deltashare-workflow-db \
  --database-name deltashare_workflow

# Configure firewall (or use VNet)
az postgres flexible-server firewall-rule create \
  --resource-group <your-rg> \
  --name deltashare-workflow-db \
  --rule-name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Get connection string
az postgres flexible-server show-connection-string \
  --server-name deltashare-workflow-db \
  --database-name deltashare_workflow \
  --admin-user dbadmin \
  --admin-password <password>
```

#### 2. Azure Storage Queue

```bash
# Create storage account (if not exists)
az storage account create \
  --name deltashareworkflow \
  --resource-group <your-rg> \
  --location eastus \
  --sku Standard_LRS

# Get connection string
az storage account show-connection-string \
  --name deltashareworkflow \
  --resource-group <your-rg> \
  --output tsv

# Queue will be auto-created on first use
```

### Azure Web App Configuration

1. **Navigate to Azure Portal** > Your Web App
2. **Configuration** > **Application settings**
3. **Add the following:**

```
enable_workflow = true
domain_db_connection_string = postgresql://dbadmin:<password>@deltashare-workflow-db.postgres.database.azure.com:5432/deltashare_workflow?sslmode=require
azure_queue_connection_string = DefaultEndpointsProtocol=https;AccountName=deltashareworkflow;AccountKey=...;EndpointSuffix=core.windows.net
azure_queue_name = sharepack-processing
```

4. **Save** and restart the app

### Deploy Application

```bash
# Build and deploy
cd /home/nitinkeshav/JLLT-EDP-DELTASHARE/api_layer
make build

# Deploy to Azure (using Azure CLI)
az webapp deployment source config-zip \
  --resource-group <your-rg> \
  --name <your-app-name> \
  --src dist/jllt_edp_deltashare-*.whl
```

### Post-Deployment Verification

1. **Check application logs:**
   ```bash
   az webapp log tail \
     --resource-group <your-rg> \
     --name <your-app-name>
   ```

2. **Test health endpoint:**
   ```bash
   curl https://your-app.azurewebsites.net/workflow/health \
     -H "X-Workspace-URL: https://your-workspace.azuredatabricks.net"
   ```

3. **Verify database tables created:**
   ```sql
   psql -h deltashare-workflow-db.postgres.database.azure.com \
        -U dbadmin \
        -d deltashare_workflow \
        -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'deltashare';"
   ```

---

## Phase 7: Monitoring & Observability (Next Week)

### Application Insights

1. **Enable Application Insights** on your Azure Web App
2. **Key metrics to monitor:**
   - Request count (`/workflow/sharepack/upload`)
   - Response times
   - Error rate (4xx, 5xx)
   - Queue processing duration

### Custom Queries

**Application Insights query for workflow requests:**
```kusto
requests
| where url contains "/workflow/"
| summarize count() by name, resultCode
| order by count_ desc
```

**Queue processing duration:**
```kusto
traces
| where message contains "Share pack provisioned"
| extend duration = extract(@"duration: (\d+)", 1, message)
| summarize avg(todouble(duration)) by bin(timestamp, 1h)
```

### Alerts

Create alerts for:
1. **Failed provisioning** - when share pack status = FAILED
2. **High error rate** - >5% of requests return 5xx
3. **Queue backlog** - >100 messages in queue
4. **Database connection failures**

### Database Monitoring

```sql
-- Monitor share pack processing
SELECT
    share_pack_status,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (effective_to - effective_from))) as avg_duration_seconds
FROM deltashare.share_packs
WHERE effective_from > NOW() - INTERVAL '24 hours'
GROUP BY share_pack_status;

-- Failed share packs in last 24 hours
SELECT
    share_pack_name,
    error_message,
    effective_from
FROM deltashare.share_packs
WHERE share_pack_status = 'FAILED'
  AND effective_from > NOW() - INTERVAL '24 hours'
ORDER BY effective_from DESC;

-- Queue processing rate
SELECT
    DATE_TRUNC('hour', effective_from) as hour,
    COUNT(*) as processed_count
FROM deltashare.share_packs
WHERE effective_from > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

---

## Phase 8: Optional Enhancements (Future)

### 1. Add Validators (Phase 2)

Implement skipped validators:

```bash
# Files to implement:
api_layer/src/dbrx_api/workflow/validators/
├── ad_validator.py           # Verify AD users/groups via Graph API
├── databricks_validator.py   # Verify Databricks objects exist
├── business_validator.py     # Business rules (unique names, etc.)
└── validation_runner.py      # Orchestrate all validations
```

**Configuration needed:**
```env
azure_tenant_id=your-tenant-id
graph_client_id=your-client-id
graph_client_secret=your-client-secret
```

### 2. Add Sync System (Phase 3)

Implement scheduled syncs:

```bash
# Files to implement:
api_layer/src/dbrx_api/workflow/sync/
├── sync_worker.py           # Separate process for syncing
├── ad_sync.py               # Sync AD users/groups
├── databricks_sync.py       # Sync Databricks objects
├── metrics_collector.py     # Collect pipeline metrics
└── notification_sender.py   # Send email notifications
```

**Deployment:**
Deploy sync worker as separate Azure Container Instance.

### 3. Add Comprehensive Tests (Phase 4)

```bash
# Create test files:
api_layer/tests/workflow/
├── test_models.py            # Pydantic model validation
├── test_parsers.py           # YAML/Excel parsing
├── test_repositories.py      # Database operations
├── test_orchestrator.py      # Provisioning flow
├── test_api.py               # API endpoints
└── fixtures/
    ├── sample.yaml
    └── sample.xlsx
```

Run tests:
```bash
make test
```

### 4. Add Rollback Capability

Allow reverting to previous share pack version:

```sql
-- Get previous version
SELECT * FROM deltashare.share_packs
WHERE share_pack_id = 'uuid-here'
  AND version = (SELECT MAX(version) - 1 FROM deltashare.share_packs WHERE share_pack_id = 'uuid-here');

-- Trigger rollback (future feature)
POST /workflow/sharepack/{id}/rollback
```

---

## Troubleshooting Guide

### Issue: "Workflow system disabled"

**Symptom:** Workflow endpoints return 404

**Fix:**
```env
# Add to .env or Azure App Settings
enable_workflow=true
```

### Issue: "Database connection failed"

**Symptom:** App crashes on startup with PostgreSQL error

**Checks:**
1. Database exists: `psql -h host -U user -d dbname`
2. Connection string format correct
3. User has CREATE SCHEMA privileges
4. Firewall allows connections

**Fix:**
```sql
-- Grant privileges
GRANT CREATE ON DATABASE deltashare_workflow TO dbadmin;
```

### Issue: "Azure queue not configured"

**Symptom:** Upload works but processing doesn't happen

**Fix:**
```env
azure_queue_connection_string=DefaultEndpointsProtocol=https;...
azure_queue_name=sharepack-processing
```

### Issue: "Expected 16 tables but found N"

**Symptom:** Migration fails with table count mismatch

**Fix:**
```sql
-- Drop and recreate
DROP SCHEMA deltashare CASCADE;
-- Restart app
```

### Issue: "python-multipart not installed"

**Symptom:** File upload fails with RuntimeError

**Fix:**
```bash
pip install python-multipart
# Or
pip install -e ".[dev]"
```

---

## Success Criteria Checklist

### Phase 1: Local Testing ✅
- [ ] App starts successfully with workflow enabled
- [ ] Health endpoint returns 200 OK
- [ ] Database has 16 tables in deltashare schema
- [ ] Can upload YAML file
- [ ] Can upload Excel file
- [ ] Share pack appears in database
- [ ] Message appears in Azure queue

### Phase 2: Integration Testing
- [ ] Queue consumer processes messages
- [ ] Status endpoint shows IN_PROGRESS
- [ ] Status endpoint shows COMPLETED or FAILED
- [ ] Database shows status updates (SCD Type 2)
- [ ] Can query share pack history

### Phase 3: Real Databricks Testing
- [ ] Recipients created in Databricks
- [ ] Shares created in Databricks
- [ ] Data objects attached to shares
- [ ] Recipients have access to shares

### Phase 4: Production Deployment
- [ ] Azure PostgreSQL deployed
- [ ] Azure Storage Queue deployed
- [ ] Web App configured with settings
- [ ] Health endpoint works in production
- [ ] Can upload share packs in production
- [ ] Monitoring and alerts configured

---

## Quick Commands Reference

```bash
# Start app locally
make run-dev

# Test health
curl http://localhost:8000/workflow/health \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"

# Upload YAML
curl -X POST http://localhost:8000/workflow/sharepack/upload \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sample_sharepack.yaml"

# Check status
curl http://localhost:8000/workflow/sharepack/{id} \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"

# Generate Excel sample
python create_sample_excel.py

# Run linting
make lint

# Database query
psql -h hostname -U username -d database_name \
  -c "SELECT COUNT(*) FROM deltashare.share_packs WHERE is_current = true;"

# Check Azure queue
az storage message peek \
  --queue-name sharepack-processing \
  --account-name <account-name>
```

---

## Support

- **Documentation:** [WORKFLOW_IMPLEMENTATION.md](WORKFLOW_IMPLEMENTATION.md)
- **Sample Files:** `sample_sharepack.yaml`, `sample_sharepack.xlsx`
- **Generator:** `create_sample_excel.py`
- **Confluence:** https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/

---

**Ready to start? Begin with Phase 1, Step 1!** 🚀
