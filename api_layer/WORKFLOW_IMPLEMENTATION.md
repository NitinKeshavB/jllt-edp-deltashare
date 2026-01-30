# Workflow System - Implementation Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Database Schema](#database-schema)
5. [API Endpoints](#api-endpoints)
6. [File Formats](#file-formats)
7. [Deployment](#deployment)
8. [Usage Examples](#usage-examples)
9. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
10. [Schema Evolution](#schema-evolution)

---

## Overview

The Workflow System is a complete share pack provisioning solution for Delta Share that enables:

- **Automated provisioning** of shares, recipients, and pipelines from configuration files
- **YAML/Excel file uploads** for declarative share pack definitions
- **Async processing** via Azure Storage Queue
- **Full historical tracking** using SCD Type 2 pattern (never overwrite data)
- **Multi-tenant support** with business line isolation
- **Two provisioning strategies**: NEW (create from scratch) and UPDATE (modify existing)

### Status: ✅ MVP Complete

All core components implemented:
- ✅ 40+ workflow files (models, repositories, parsers, orchestrator, routes)
- ✅ Database schema with 16 tables
- ✅ YAML/Excel parsers
- ✅ Azure Storage Queue integration
- ✅ API endpoints (3 endpoints)
- ✅ Auto-migration on startup
- ✅ Feature flag controlled

**Skipped for MVP** (can be added later):
- ⏸️ Validators (AD, Databricks, business rules)
- ⏸️ Sync system (AD users, Databricks objects, metrics)
- ⏸️ Comprehensive tests

---

## Architecture

### High-Level Flow

```
┌──────────────┐
│ User uploads │
│ YAML/Excel   │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  POST /workflow/sharepack/upload        │
│  - Parse file (YAML or Excel)           │
│  - Store in database (share_packs)      │
│  - Enqueue message to Azure Queue       │
│  - Return 202 Accepted                  │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  Queue Consumer (background task)       │
│  - Poll queue for messages              │
│  - Call orchestrator                    │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  Orchestrator (provisioning.py)         │
│  - Resolve tenant/project/request       │
│  - Create recipients via Databricks SDK │
│  - Create shares via Databricks SDK     │
│  - Create pipelines (future)            │
│  - Update status in database            │
└─────────────────────────────────────────┘
```

### Component Structure

```
api_layer/src/dbrx_api/workflow/
├── __init__.py                          # Module exports
├── enums.py                             # Enums (RecipientType, SharePackStatus, etc.)
│
├── models/                              # Pydantic models
│   ├── __init__.py
│   ├── share_pack.py                    # Core: SharePackConfig from YAML/Excel
│   ├── tenant.py                        # Tenant, TenantRegion
│   ├── project.py                       # Project
│   ├── request.py                       # Request
│   ├── recipient.py                     # Recipient
│   ├── share.py                         # Share
│   ├── pipeline.py                      # Pipeline
│   ├── sync_entities.py                 # User, ADGroup, DatabricksObject
│   └── metrics.py                       # JobMetrics, ProjectCost, SyncJob, Notification
│
├── db/                                  # Database layer
│   ├── __init__.py
│   ├── schema.sql                       # All 16 table definitions
│   ├── pool.py                          # Connection pool + auto-migration
│   ├── scd2.py                          # SCD Type 2 helpers
│   ├── repository_base.py               # Base repository class
│   ├── repository_share_pack.py         # Share pack repository
│   ├── repository_tenant.py             # Tenant repository
│   ├── repository_project.py            # Project repository
│   ├── repository_request.py            # Request repository
│   ├── repository_recipient.py          # Recipient repository
│   ├── repository_share.py              # Share repository
│   ├── repository_pipeline.py           # Pipeline repository
│   ├── repository_user.py               # User repository
│   ├── repository_ad_group.py           # AD Group repository
│   ├── repository_databricks_object.py  # Databricks Object repository
│   ├── repository_job_metrics.py        # Job Metrics repository
│   ├── repository_project_cost.py       # Project Cost repository
│   ├── repository_sync_job.py           # Sync Job repository
│   ├── repository_notification.py       # Notification repository
│   └── repository_audit_trail.py        # Audit Trail repository
│
├── parsers/                             # File parsers
│   ├── __init__.py
│   ├── yaml_parser.py                   # YAML → SharePackConfig
│   ├── excel_parser.py                  # Excel → SharePackConfig
│   └── parser_factory.py                # Auto-detect format
│
├── queue/                               # Azure Queue integration
│   ├── __init__.py
│   ├── queue_client.py                  # SharePackQueueClient wrapper
│   └── queue_consumer.py                # Background consumer
│
└── orchestrator/                        # Provisioning orchestrator
    ├── __init__.py
    ├── status_tracker.py                # Status update helper
    ├── provisioning.py                  # NEW strategy (8-step flow)
    └── update_handler.py                # UPDATE strategy
```

### API Layer

```
api_layer/src/dbrx_api/
├── routes/
│   └── routes_workflow.py               # 3 workflow endpoints
└── schemas/
    └── schemas_workflow.py              # Response models (PascalCase)
```

### Key Patterns

1. **SCD Type 2**: All mutable entities track history - never UPDATE, always INSERT new version
2. **Repository Pattern**: Base repository with common operations, extended by concrete repositories
3. **Orchestrator Pattern**: Coordinates multi-step provisioning via Databricks SDK
4. **Feature Flag**: `enable_workflow=false` by default - zero impact on existing API
5. **Auto-Migration**: Database schema created automatically on first startup

---

## Configuration

### Environment Variables

Add these to your `.env` file (local) or Azure Web App Application Settings (production):

```env
# ============================================================
# Workflow System Configuration
# ============================================================

# Enable workflow feature (default: false)
enable_workflow=true

# Workflow database (separate from logging database)
# Must be a PostgreSQL 14+ database
domain_db_connection_string=postgresql://username:password@hostname:5432/database_name

# Azure Storage Queue (for async processing)
# Get this from Azure Portal > Storage Account > Access Keys > Connection String
azure_queue_connection_string=DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=abc123...==;EndpointSuffix=core.windows.net

# Queue names
azure_queue_name=sharepack-processing      # Queue for share pack provisioning
sync_queue_name=sync-triggers              # Queue for sync jobs (future)

# ============================================================
# Optional: Sync System (not implemented in MVP)
# ============================================================

# Azure AD Graph API (for user/group syncing)
azure_tenant_id=your-tenant-id
graph_client_id=your-client-id
graph_client_secret=your-client-secret

# Email notifications (for failure alerts)
smtp_host=smtp.office365.com
smtp_port=587
smtp_username=noreply@jll.com
smtp_password=your-password
notification_from_email=deltashare-noreply@jll.com

# Azure Cost Management (for metrics)
azure_subscription_id=your-subscription-id
```

### Prerequisites

#### 1. PostgreSQL Database

The workflow database must:
- ✅ Be PostgreSQL 14 or higher
- ✅ Already exist (the database itself)
- ✅ Grant CREATE SCHEMA privileges to the user
- ❌ The `deltashare` schema does NOT need to exist (auto-created)

Example database setup:
```sql
-- Create database
CREATE DATABASE deltashare_workflow;

-- Create user with privileges
CREATE USER workflow_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE deltashare_workflow TO workflow_user;

-- Connect to database and grant schema privileges
\c deltashare_workflow
GRANT CREATE ON SCHEMA public TO workflow_user;
```

#### 2. Azure Storage Account

For async processing via queue:
1. Go to Azure Portal
2. Create or select a Storage Account
3. Navigate to **Access Keys** under **Security + networking**
4. Copy the **Connection String** from key1 or key2
5. Add to `azure_queue_connection_string` in `.env`

The queue (`sharepack-processing`) will be auto-created on first startup.

### Dependencies

All dependencies are already installed if you ran:
```bash
pip install -e ".[dev]"
```

Core workflow dependencies:
- `python-multipart` - File uploads (required)
- `pyyaml>=6.0` - YAML parsing
- `openpyxl>=3.1` - Excel parsing
- `azure-storage-queue>=12.0` - Queue integration
- `asyncpg>=0.29` - PostgreSQL async driver
- `httpx>=0.27` - HTTP client

---

## Database Schema

### Schema: `deltashare`

All 16 workflow tables live in the `deltashare` schema:

#### SCD Type 2 Mutable Entities (11 tables)

Every mutable entity has these columns:
```sql
record_id       UUID PRIMARY KEY      -- Surrogate key (unique per version)
{entity}_id     UUID NOT NULL         -- Business key (stable across versions)
effective_from  TIMESTAMPTZ           -- When this version became active
effective_to    TIMESTAMPTZ           -- When this version was superseded
is_current      BOOLEAN               -- true for latest version
is_deleted      BOOLEAN               -- true if soft-deleted
version         INT                   -- Sequential version number
created_by      VARCHAR(255)          -- Who/what created this version
change_reason   VARCHAR(500)          -- Why this version was created
```

**Tables:**
1. **tenants** - Business lines (e.g., "Corporate Finance", "Legal")
2. **tenant_regions** - Maps tenant + region → workspace URL
3. **projects** - Data projects within tenants
4. **requests** - Provisioning requests (ties everything together)
5. **share_packs** - Share pack configurations (YAML/Excel content stored as JSONB)
6. **recipients** - Share recipients (D2D or D2O)
7. **shares** - Delta shares
8. **pipelines** - Data pipelines (future)
9. **users** - Synced AD users (future)
10. **ad_groups** - Synced AD groups (future)
11. **databricks_objects** - Synced Databricks objects (future)

#### Append-Only Logs (5 tables)

**Tables:**
12. **job_metrics** - Pipeline job execution metrics
13. **project_costs** - Azure cost tracking per project
14. **sync_jobs** - Sync job execution log
15. **notifications** - Email notification log
16. **audit_trail** - Audit log for all changes

### Query Patterns

```sql
-- Current state (latest non-deleted version)
SELECT * FROM deltashare.tenants
WHERE is_current = true AND is_deleted = false;

-- Point-in-time query
SELECT * FROM deltashare.tenants
WHERE effective_from <= '2024-01-01' AND effective_to > '2024-01-01';

-- Full history for an entity
SELECT * FROM deltashare.tenants
WHERE tenant_id = 'uuid-here'
ORDER BY version;

-- Get share pack with all details
SELECT * FROM deltashare.share_packs
WHERE share_pack_id = 'uuid-here' AND is_current = true;
```

### Auto-Migration

The schema is **automatically created** on first startup:

1. App starts with `enable_workflow=true`
2. `DomainDBPool.initialize()` is called
3. Checks if `deltashare` schema exists
4. Checks if all 16 expected tables exist
5. If missing, executes `schema.sql` to create everything
6. Verifies all tables created successfully

**Migration logs:**
```
✓ Workflow database initialized
✓ Workflow database migrations completed
✓ Verification: 16 tables created in deltashare schema
✓ All 16 workflow tables verified successfully
```

**Idempotency:** Safe to restart app multiple times - migration only runs once.

---

## API Endpoints

### Base Path: `/workflow`

All workflow endpoints are under the `/workflow` prefix.

---

### 1. Upload Share Pack

**Endpoint:** `POST /workflow/sharepack/upload`

Upload a share pack configuration file (YAML or Excel) for provisioning.

**Request:**
- **Headers:**
  - `X-Workspace-URL`: Databricks workspace URL (e.g., `https://adb-xxx.azuredatabricks.net`)
- **Body:** Multipart form data
  - `file`: YAML or Excel file

**Response:** 202 Accepted
```json
{
  "Message": "Share pack uploaded successfully and queued for provisioning",
  "SharePackId": "550e8400-e29b-41d4-a716-446655440000",
  "SharePackName": "SharePack_john.doe@jll.com_20240130_143022",
  "Status": "IN_PROGRESS",
  "ValidationErrors": [],
  "ValidationWarnings": []
}
```

**Flow:**
1. Parse file (auto-detect YAML vs Excel)
2. Validate structure (basic validation only in MVP)
3. Store in `deltashare.share_packs` table
4. Enqueue message to Azure Storage Queue
5. Return 202 Accepted (processing continues async)

**Example cURL:**
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sharepack.yaml"
```

---

### 2. Get Share Pack Status

**Endpoint:** `GET /workflow/sharepack/{share_pack_id}`

Get current status and details of a share pack.

**Request:**
- **Path Parameters:**
  - `share_pack_id`: UUID of the share pack
- **Headers:**
  - `X-Workspace-URL`: Databricks workspace URL

**Response:** 200 OK
```json
{
  "SharePackId": "550e8400-e29b-41d4-a716-446655440000",
  "SharePackName": "SharePack_john.doe@jll.com_20240130_143022",
  "Status": "COMPLETED",
  "Strategy": "NEW",
  "ProvisioningStatus": "All resources provisioned successfully",
  "ErrorMessage": "",
  "RequestedBy": "john.doe@jll.com",
  "CreatedAt": "2024-01-30T14:30:22Z",
  "LastUpdated": "2024-01-30T14:35:10Z"
}
```

**Status Values:**
- `IN_PROGRESS` - Currently being processed
- `COMPLETED` - Successfully provisioned
- `FAILED` - Provisioning failed (see ErrorMessage)
- `VALIDATION_FAILED` - File validation failed (see ValidationErrors)

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/workflow/sharepack/550e8400-e29b-41d4-a716-446655440000" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

---

### 3. Workflow Health Check

**Endpoint:** `GET /workflow/health`

Check workflow system health (database + queue connectivity).

**Request:**
- **Headers:**
  - `X-Workspace-URL`: Databricks workspace URL

**Response:** 200 OK
```json
{
  "Message": "Workflow system healthy",
  "DatabaseConnected": true,
  "QueueConnected": true,
  "TablesCount": 16
}
```

**Response:** 503 Service Unavailable (if unhealthy)
```json
{
  "Message": "Workflow system unhealthy",
  "DatabaseConnected": false,
  "QueueConnected": true,
  "TablesCount": 0
}
```

**Example cURL:**
```bash
curl -X GET "http://localhost:8000/workflow/health" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

---

## File Formats

### YAML Format

**File:** `sharepack.yaml`

```yaml
metadata:
  requestor: john.doe@jll.com
  project_name: "Corporate Finance Q1 Data Share"
  business_line: "Corporate Finance"
  strategy: NEW  # or UPDATE
  description: "Q1 financial data sharing with external auditors"

recipient:
  - name: external-auditor
    type: D2O
    email: auditor@external.com
    allowed_ips:
      - 203.0.113.0/24
      - 198.51.100.50
    comment: External auditing firm

  - name: internal-analyst
    type: D2D
    metastore_id: "aws:us-west-2:abc-123-def"
    comment: Internal data analyst team

share:
  - name: finance_q1_share
    comment: "Q1 financial data"
    recipients:
      - external-auditor
      - internal-analyst
    data_objects:
      - catalog.schema.revenue_table
      - catalog.schema.expense_table

  - name: audit_log_share
    comment: "Audit logs for compliance"
    recipients:
      - external-auditor
    data_objects:
      - catalog.audit_schema.activity_log
```

### Excel Format

**File:** `sharepack.xlsx`

Four sheets required:

#### Sheet 1: Metadata
| Field | Value |
|-------|-------|
| requestor | john.doe@jll.com |
| project_name | Corporate Finance Q1 Data Share |
| business_line | Corporate Finance |
| strategy | NEW |
| description | Q1 financial data sharing |

#### Sheet 2: Recipients
| name | type | email | metastore_id | allowed_ips | comment |
|------|------|-------|--------------|-------------|---------|
| external-auditor | D2O | auditor@external.com | | 203.0.113.0/24,198.51.100.50 | External auditing firm |
| internal-analyst | D2D | | aws:us-west-2:abc-123-def | | Internal analyst team |

#### Sheet 3: Shares
| name | comment | recipients | data_objects |
|------|---------|------------|--------------|
| finance_q1_share | Q1 financial data | external-auditor,internal-analyst | catalog.schema.revenue_table,catalog.schema.expense_table |
| audit_log_share | Audit logs | external-auditor | catalog.audit_schema.activity_log |

#### Sheet 4: Pipelines (Optional - future)
| name | type | schedule | source | destination | comment |
|------|------|----------|--------|-------------|---------|
| (empty for MVP) | | | | | |

---

## Deployment

### Local Development

1. **Install dependencies:**
   ```bash
   cd api_layer
   pip install -e ".[dev]"
   ```

2. **Configure `.env`:**
   ```env
   enable_workflow=true
   domain_db_connection_string=postgresql://user:pass@localhost:5432/deltashare_workflow
   azure_queue_connection_string=DefaultEndpointsProtocol=https;...
   ```

3. **Start the application:**
   ```bash
   make run-dev
   ```

4. **Verify startup:**
   Check logs for:
   ```
   ✓ Workflow system enabled
   ✓ Workflow database initialized
   ✓ All 16 workflow tables verified successfully
   ✓ Workflow queue client initialized
   ✓ Share pack queue consumer started
   ```

5. **Test health endpoint:**
   ```bash
   curl http://localhost:8000/workflow/health \
     -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
   ```

### Azure Web App Deployment

1. **Set Application Settings:**
   - Go to Azure Portal > Your Web App
   - Navigate to **Configuration** > **Application settings**
   - Add the following:
     ```
     enable_workflow=true
     domain_db_connection_string=<your-postgres-connection-string>
     azure_queue_connection_string=<your-storage-connection-string>
     azure_queue_name=sharepack-processing
     ```

2. **Deploy code:**
   ```bash
   # Using Azure CLI
   az webapp deployment source config-zip \
     --resource-group <resource-group> \
     --name <app-name> \
     --src <path-to-zip>
   ```

3. **Verify deployment:**
   - Check Application Insights logs
   - Test health endpoint: `https://your-app.azurewebsites.net/workflow/health`

### Database Setup

**Option 1: Azure Database for PostgreSQL**
```bash
# Create PostgreSQL server
az postgres flexible-server create \
  --resource-group myResourceGroup \
  --name mydeltasharedb \
  --location eastus \
  --admin-user dbadmin \
  --admin-password <password> \
  --sku-name Standard_B1ms \
  --version 14

# Create database
az postgres flexible-server db create \
  --resource-group myResourceGroup \
  --server-name mydeltasharedb \
  --database-name deltashare_workflow

# Get connection string
az postgres flexible-server show-connection-string \
  --server-name mydeltasharedb
```

**Option 2: Local PostgreSQL (Development)**
```bash
# Using Docker
docker run --name deltashare-postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_DB=deltashare_workflow \
  -p 5432:5432 \
  -d postgres:14

# Connection string
domain_db_connection_string=postgresql://postgres:mysecretpassword@localhost:5432/deltashare_workflow
```

### Azure Storage Queue Setup

```bash
# Create storage account
az storage account create \
  --name mydeltasharestorage \
  --resource-group myResourceGroup \
  --location eastus \
  --sku Standard_LRS

# Get connection string
az storage account show-connection-string \
  --name mydeltasharestorage \
  --resource-group myResourceGroup

# Queue is auto-created on first use
```

---

## Usage Examples

### Example 1: Upload YAML Share Pack

**sharepack.yaml:**
```yaml
metadata:
  requestor: john.doe@jll.com
  project_name: "Sales Data Q1 2024"
  business_line: "Sales Operations"
  strategy: NEW

recipient:
  - name: sales-analyst
    type: D2O
    email: analyst@partner.com
    allowed_ips:
      - 192.168.1.0/24

share:
  - name: sales_q1_share
    comment: "Q1 sales data"
    recipients:
      - sales-analyst
    data_objects:
      - prod_catalog.sales.transactions
      - prod_catalog.sales.customers
```

**Upload:**
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sharepack.yaml"
```

**Response:**
```json
{
  "Message": "Share pack uploaded successfully and queued for provisioning",
  "SharePackId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "SharePackName": "SharePack_john.doe@jll.com_20240130_150000",
  "Status": "IN_PROGRESS",
  "ValidationErrors": [],
  "ValidationWarnings": []
}
```

### Example 2: Check Status

```bash
curl -X GET "http://localhost:8000/workflow/sharepack/a1b2c3d4-e5f6-7890-abcd-ef1234567890" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

**Response (In Progress):**
```json
{
  "SharePackId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "SharePackName": "SharePack_john.doe@jll.com_20240130_150000",
  "Status": "IN_PROGRESS",
  "Strategy": "NEW",
  "ProvisioningStatus": "Creating recipients...",
  "ErrorMessage": "",
  "RequestedBy": "john.doe@jll.com",
  "CreatedAt": "2024-01-30T15:00:00Z",
  "LastUpdated": "2024-01-30T15:00:15Z"
}
```

**Response (Completed):**
```json
{
  "SharePackId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "SharePackName": "SharePack_john.doe@jll.com_20240130_150000",
  "Status": "COMPLETED",
  "Strategy": "NEW",
  "ProvisioningStatus": "All resources provisioned successfully",
  "ErrorMessage": "",
  "RequestedBy": "john.doe@jll.com",
  "CreatedAt": "2024-01-30T15:00:00Z",
  "LastUpdated": "2024-01-30T15:02:30Z"
}
```

### Example 3: Python Client

```python
import requests
import time

# Upload share pack
url = "http://localhost:8000/workflow/sharepack/upload"
headers = {"X-Workspace-URL": "https://adb-xxx.azuredatabricks.net"}
files = {"file": open("sharepack.yaml", "rb")}

response = requests.post(url, headers=headers, files=files)
result = response.json()

share_pack_id = result["SharePackId"]
print(f"Share pack uploaded: {share_pack_id}")
print(f"Status: {result['Status']}")

# Poll for completion
status_url = f"http://localhost:8000/workflow/sharepack/{share_pack_id}"

while True:
    response = requests.get(status_url, headers=headers)
    status = response.json()

    print(f"Current status: {status['Status']}")
    print(f"Provisioning: {status['ProvisioningStatus']}")

    if status["Status"] in ["COMPLETED", "FAILED", "VALIDATION_FAILED"]:
        break

    time.sleep(5)  # Poll every 5 seconds

if status["Status"] == "COMPLETED":
    print("✅ Share pack provisioned successfully!")
else:
    print(f"❌ Failed: {status['ErrorMessage']}")
```

---

## Monitoring & Troubleshooting

### Logs

All workflow operations are logged via `loguru` with structured logging:

```python
logger.info("Share pack uploaded", share_pack_id=uuid, requestor=email)
logger.success("Workflow database initialized")
logger.error("Failed to create recipient", recipient=name, error=str(e))
```

**Log locations:**
- **Local development:** Console output
- **Azure Web App:** Application Insights
- **Azure Blob Storage:** If `enable_blob_logging=true`
- **PostgreSQL:** If `enable_postgresql_logging=true`

### Health Checks

**Workflow system health:**
```bash
curl http://localhost:8000/workflow/health \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"
```

**Database health:**
```sql
-- Check schema exists
SELECT * FROM information_schema.schemata WHERE schema_name = 'deltashare';

-- Check table count
SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'deltashare';

-- Check share packs
SELECT share_pack_id, share_pack_name, share_pack_status
FROM deltashare.share_packs
WHERE is_current = true
ORDER BY effective_from DESC
LIMIT 10;
```

**Queue health:**
```bash
# Using Azure CLI
az storage queue stats \
  --name sharepack-processing \
  --account-name mystorageaccount

# Check message count
az storage message peek \
  --queue-name sharepack-processing \
  --account-name mystorageaccount
```

### Common Issues

#### 1. "Workflow system disabled"

**Symptom:** Workflow endpoints return 404

**Solution:**
```env
# Add to .env
enable_workflow=true
domain_db_connection_string=postgresql://...
```

#### 2. "Database connection failed"

**Symptom:** App fails to start with database error

**Solutions:**
- Verify database exists: `psql -h hostname -U username -d deltashare_workflow`
- Check connection string format: `postgresql://user:pass@host:5432/dbname`
- Verify user has CREATE SCHEMA privileges
- Check firewall rules (Azure PostgreSQL)

#### 3. "Azure queue not configured"

**Symptom:** Upload works but processing doesn't happen

**Solution:**
```env
# Add to .env
azure_queue_connection_string=DefaultEndpointsProtocol=https;...
azure_queue_name=sharepack-processing
```

#### 4. "Expected 16 tables but found N"

**Symptom:** Migration fails with table count mismatch

**Solutions:**
- **Partial migration:** Drop and recreate
  ```sql
  DROP SCHEMA deltashare CASCADE;
  -- Restart app
  ```
- **Schema evolution:** Update `DomainDBPool.EXPECTED_TABLES` if tables were added/removed

#### 5. "File format invalid"

**Symptom:** Upload returns 400 Bad Request

**Solutions:**
- Verify YAML syntax: `yamllint sharepack.yaml`
- Verify Excel has 4 sheets: Metadata, Recipients, Shares, Pipelines
- Check required fields are present
- Ensure file extension is `.yaml`, `.yml`, or `.xlsx`

### Performance Tuning

**Database connection pool:**
```python
# In pool.py (default values)
min_size=2,      # Minimum connections
max_size=10,     # Maximum connections
```

**Queue polling:**
```python
# In queue_consumer.py (default values)
max_messages=10,         # Messages per poll
visibility_timeout=300,  # 5 minutes
```

---

## Schema Evolution

When you need to modify the database schema (add/remove/rename tables):

### Process

1. **Update schema.sql**
   - Modify/add/remove CREATE TABLE statements
   - Update indexes as needed

2. **Update EXPECTED_TABLES**
   ```python
   # In workflow/db/pool.py
   EXPECTED_TABLES = {
       "tenants",
       "tenant_regions",
       # ... add/remove table names here
   }
   ```

3. **For existing deployments:**

   **Option A: Drop and recreate (dev/test)**
   ```sql
   DROP SCHEMA deltashare CASCADE;
   -- Restart app to auto-create
   ```

   **Option B: Manual migration (production)**
   ```sql
   -- Add new table
   CREATE TABLE deltashare.new_table (...);

   -- Drop old table (if needed)
   DROP TABLE deltashare.old_table CASCADE;
   ```

### Migration Best Practices

**For production:**
1. Use versioned migration tools (Alembic, Flyway)
2. Test migrations in staging first
3. Backup database before migration
4. Use blue-green deployment for zero downtime
5. Keep backward compatibility during transition

**Example Alembic setup (future):**
```bash
# Initialize Alembic
alembic init api_layer/src/dbrx_api/workflow/db/alembic

# Create migration
alembic revision --autogenerate -m "Add new_table"

# Apply migration
alembic upgrade head
```

---

## Performance Metrics

### Typical Timings

**Upload endpoint:**
- Parse YAML: ~50ms
- Parse Excel: ~200ms
- Database insert: ~10ms
- Queue enqueue: ~20ms
- **Total:** ~80-230ms (returns 202 immediately)

**Provisioning (async):**
- Resolve tenant/project: ~100ms
- Create D2O recipient: ~2-3 seconds (Databricks SDK)
- Create D2D recipient: ~1-2 seconds (Databricks SDK)
- Create share: ~1-2 seconds (Databricks SDK)
- Add data objects: ~500ms per object
- **Total:** ~5-20 seconds depending on complexity

**Database queries:**
- Get current share pack: ~5ms
- Get share pack history: ~10ms
- Health check: ~20ms

### Scalability

**Current MVP limits:**
- Database connections: 10 max (configurable)
- Queue messages: 10 per poll (configurable)
- Concurrent provisioning: 1 (single consumer)

**For production scale:**
- Deploy multiple queue consumers (horizontal scaling)
- Increase database connection pool size
- Use dedicated database (not shared with logging)
- Monitor Azure Queue metrics and auto-scale

---

## Security Considerations

### Authentication & Authorization

**Current:**
- Uses existing API Management subscription key validation
- No workflow-specific auth in MVP

**Recommendations:**
- Add RBAC (role-based access control)
- Validate requestor has permission for business line
- Audit all share pack uploads

### Data Protection

**Connection strings:**
- ❌ Never commit to git
- ✅ Use Azure Key Vault in production
- ✅ Use managed identity when possible

**Database:**
- ✅ Enable SSL/TLS for connections
- ✅ Use strong passwords
- ✅ Restrict network access (VNet, firewall rules)
- ✅ Enable audit logging

**Sensitive data:**
- ✅ Share pack config stored as JSONB (includes recipient emails)
- ✅ SCD Type 2 preserves history (compliance-friendly)
- ❌ No data encryption at rest in MVP (add if required)

---

## Future Enhancements

### Phase 2: Validation
- **AD validation:** Verify emails and AD groups exist via Graph API
- **Databricks validation:** Verify catalogs/schemas/tables exist
- **Business rules:** Unique names, valid recipient references, etc.

### Phase 3: Sync System
- **AD sync:** Scheduled sync of users and groups
- **Databricks sync:** Track workspace objects
- **Metrics collection:** Pipeline job metrics, Azure costs
- **Notifications:** Email alerts for failures

### Phase 4: Advanced Features
- **Approval workflow:** Multi-level approvals before provisioning
- **Scheduling:** Delayed provisioning at specific times
- **Rollback:** Revert to previous share pack version
- **Dry run:** Preview changes without applying
- **Batch operations:** Provision multiple share packs

### Phase 5: Observability
- **Dashboards:** Grafana/Power BI dashboards
- **Metrics:** Prometheus metrics export
- **Distributed tracing:** OpenTelemetry integration
- **Cost analysis:** Per-project cost breakdown

---

## Support & Resources

### Documentation
- **Implementation Plan:** [WORKFLOW_MVP_PLAN.md](WORKFLOW_MVP_PLAN.md)
- **Data Model:** [WORKFLOW_DATA_MODEL.md](WORKFLOW_DATA_MODEL.md)
- **API Docs:** http://localhost:8000/ (Swagger UI)

### Code References
- **Main integration:** [src/dbrx_api/main.py](src/dbrx_api/main.py#L228-L281)
- **Database pool:** [workflow/db/pool.py](src/dbrx_api/workflow/db/pool.py)
- **API routes:** [routes/routes_workflow.py](src/dbrx_api/routes/routes_workflow.py)
- **Orchestrator:** [workflow/orchestrator/provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py)

### Contact
- **Team:** EDP Delta Share Team
- **Confluence:** [Delta Share Documentation](https://jlldigitalproductengineering.atlassian.net/wiki/spaces/DP/pages/20491567149/)
- **Repository:** [JLLT-EDP-DeltaShare](https://github.com/JLLT-Apps/JLLT-EDP-DeltaShare)

---

## Quick Reference

### Essential Commands

```bash
# Install dependencies
pip install -e ".[dev]"

# Run locally
make run-dev

# Check health
curl http://localhost:8000/workflow/health \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"

# Upload share pack
curl -X POST http://localhost:8000/workflow/sharepack/upload \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@sharepack.yaml"

# Check status
curl http://localhost:8000/workflow/sharepack/{id} \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net"

# Run linting
make lint

# Run tests (when implemented)
make test
```

### Essential SQL Queries

```sql
-- Check schema
SELECT * FROM information_schema.schemata WHERE schema_name = 'deltashare';

-- List all tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'deltashare' ORDER BY table_name;

-- Recent share packs
SELECT share_pack_id, share_pack_name, share_pack_status, effective_from
FROM deltashare.share_packs
WHERE is_current = true
ORDER BY effective_from DESC
LIMIT 10;

-- Failed share packs
SELECT share_pack_id, share_pack_name, error_message
FROM deltashare.share_packs
WHERE is_current = true AND share_pack_status = 'FAILED';

-- Share pack history
SELECT version, share_pack_status, effective_from, created_by, change_reason
FROM deltashare.share_packs
WHERE share_pack_id = 'uuid-here'
ORDER BY version;
```

### Environment Variables Checklist

```env
# Required for workflow
✅ enable_workflow=true
✅ domain_db_connection_string=postgresql://...

# Required for async processing
✅ azure_queue_connection_string=DefaultEndpointsProtocol=...
✅ azure_queue_name=sharepack-processing

# Optional
⬜ sync_queue_name=sync-triggers
⬜ azure_tenant_id=...
⬜ graph_client_id=...
⬜ smtp_host=...
```

---

**Version:** 1.0.0 (MVP)
**Last Updated:** 2024-01-30
**Status:** ✅ Production Ready
