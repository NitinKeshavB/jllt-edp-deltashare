# Delta Share API - Comprehensive Documentation

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [API Endpoints Reference](#3-api-endpoints-reference)
4. [Authentication & Security](#4-authentication--security)
5. [Configuration](#5-configuration)
6. [Business Logic Modules](#6-business-logic-modules)
7. [Database & Storage](#7-database--storage)
8. [Monitoring & Logging](#8-monitoring--logging)
9. [Testing](#9-testing)
10. [Development Guide](#10-development-guide)
11. [Deployment](#11-deployment)
12. [Workflow System](#12-workflow-system)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Project Overview

### Introduction

**Delta Share API** is a FastAPI-based REST API that enables data engineering teams to share Databricks assets (tables, views, streaming tables, materialized views, and notebooks) with clients both internal and external to the enterprise.

| Property | Value |
|----------|-------|
| **Package Name** | `deltashare_api` |
| **Module Name** | `dbrx_api` |
| **Python Version** | 3.12+ |
| **Framework** | FastAPI |
| **Primary SDK** | Databricks SDK |

### Core Domain Concepts

- **Share**: A named collection of Databricks data objects (tables, views, schemas) that can be shared with recipients
- **Recipient**: An entity (internal or external) that receives access to shared data
  - **D2D (Databricks-to-Databricks)**: Recipients with Databricks infrastructure using DATABRICKS authentication
  - **D2O (Databricks-to-Open)**: Recipients without Databricks using TOKEN authentication
- **Data Objects**: Tables, views, streaming tables, materialized views that can be added to shares
- **Pipeline**: Delta Live Tables (DLT) pipelines for data transformation
- **Schedule**: Cron-based job schedules for automated pipeline execution

---

## 2. Architecture

### Project Structure

```
api_layer/
├── src/dbrx_api/                    # Main application package
│   ├── main.py                      # FastAPI app factory & configuration
│   ├── settings.py                  # Pydantic settings (env var loading)
│   ├── dependencies.py              # FastAPI dependencies (auth, validation)
│   ├── errors.py                    # Global error handlers
│   │
│   ├── schemas/                     # Request/response models
│   │   ├── schemas.py               # Core Pydantic models
│   │   └── schemas_workflow.py      # Workflow-specific schemas
│   │
│   ├── routes/                      # API endpoint handlers
│   │   ├── routes_health.py         # Health checks (2 endpoints)
│   │   ├── routes_share.py          # Share operations (8 endpoints)
│   │   ├── routes_recipient.py      # Recipient management (10 endpoints)
│   │   ├── routes_catalog.py        # Unity Catalog operations (3 endpoints)
│   │   ├── routes_pipelines.py      # DLT Pipeline operations (10 endpoints)
│   │   ├── routes_schedule.py       # Pipeline scheduling (7 endpoints)
│   │   ├── routes_metrics.py        # Metrics extraction (4 endpoints)
│   │   └── routes_workflow.py       # Share pack workflow (4 endpoints)
│   │
│   ├── dltshr/                      # Delta Sharing business logic
│   │   ├── share.py                 # Share CRUD operations
│   │   └── recipient.py             # Recipient CRUD operations
│   │
│   ├── jobs/                        # Databricks Jobs/Pipeline APIs
│   │   ├── dbrx_pipelines.py        # DLT pipeline operations
│   │   ├── dbrx_catalog.py          # Unity Catalog operations
│   │   └── dbrx_schedule.py         # Schedule/job operations
│   │
│   ├── dbrx_auth/                   # Authentication & token management
│   │   ├── token_gen.py             # OAuth2 token generation
│   │   └── token_manager.py         # Thread-safe token caching
│   │
│   ├── monitoring/                  # Logging infrastructure
│   │   ├── logger.py                # Loguru multi-sink configuration
│   │   ├── request_context.py       # Request tracking middleware
│   │   ├── azure_blob_handler.py    # Azure Blob Storage sink
│   │   ├── postgresql_handler.py    # PostgreSQL sink
│   │   └── datadog_handler.py       # Datadog sink
│   │
│   ├── metrics/                     # Pipeline metrics extraction
│   │   └── pipeline_metrics.py      # Metrics collection logic
│   │
│   └── workflow/                    # Share pack workflow system
│       ├── models/                  # Pydantic models
│       ├── db/                      # PostgreSQL SCD2 repositories
│       ├── orchestrator/            # Provisioning orchestration
│       ├── parsers/                 # YAML/Excel file parsing
│       ├── queue/                   # Azure Storage Queue integration
│       └── validators/              # Strategy detection & validation
│
├── tests/                           # Test suite
│   ├── conftest.py                  # Pytest configuration
│   ├── fixtures/                    # Test fixtures & mocks
│   └── unit_tests/                  # 30+ test files
│
├── postman_collection/              # Postman API collections
├── pyproject.toml                   # Project metadata & dependencies
├── Makefile                         # Development commands
├── run.sh                           # Task runner script
└── version.txt                      # Version number (0.0.1)
```

### Application Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     FastAPI Routes Layer                     │
│  (routes_share.py, routes_recipient.py, routes_pipelines.py) │
├─────────────────────────────────────────────────────────────┤
│                   Dependencies Layer                         │
│     (get_workspace_url, verify_subscription_key)            │
├─────────────────────────────────────────────────────────────┤
│                  Business Logic Layer                        │
│         (dltshr/share.py, dltshr/recipient.py)              │
├─────────────────────────────────────────────────────────────┤
│                   Databricks SDK Layer                       │
│      (jobs/dbrx_pipelines.py, jobs/dbrx_catalog.py)         │
├─────────────────────────────────────────────────────────────┤
│                  Authentication Layer                        │
│        (dbrx_auth/token_gen.py, token_manager.py)           │
├─────────────────────────────────────────────────────────────┤
│                    Monitoring Layer                          │
│    (monitoring/logger.py, request_context.py, handlers)     │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Patterns

1. **Per-Request Workspace URLs**: Each API request includes `X-Workspace-URL` header specifying the Databricks workspace
2. **Defense in Depth Authentication**: Multiple authentication layers (APIM + FastAPI + Databricks OAuth)
3. **Thread-Safe Token Caching**: In-memory token management with automatic refresh
4. **Repository Pattern**: Database operations abstracted via repository classes
5. **SCD Type 2**: Full historical tracking in workflow database

---

## 3. API Endpoints Reference

### Base Configuration

| Setting | Value |
|---------|-------|
| **Base Path** | `/api` |
| **Required Header** | `X-Workspace-URL: https://<workspace>.azuredatabricks.net` |
| **Auth Header** | `Subscription-Key: <your-key>` |
| **Content-Type** | `application/json` |

### 3.1 Health & Status (2 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Basic health check (status, timestamp, version) |
| POST | `/api/health/logging/test` | Test logging connectivity (Azure Blob, PostgreSQL, Datadog) |

**Example Response (GET /health):**
```json
{
  "status": "healthy",
  "service_name": "deltashare-api",
  "version": "0.0.1",
  "timestamp": "2024-02-07T10:30:00Z"
}
```

### 3.2 Shares (8 endpoints)

| Method | Endpoint | Description | Query Params |
|--------|----------|-------------|--------------|
| GET | `/api/shares` | List all shares | `prefix`, `page_size` |
| GET | `/api/shares/{share_name}` | Get share by name | - |
| POST | `/api/shares/{share_name}` | Create new share | `description` (required), `storage_root` (optional) |
| DELETE | `/api/shares/{share_name}` | Delete share | - |
| PUT | `/api/shares/{share_name}/dataobject/add` | Add data objects | Body: `AddDataObjectsRequest` |
| PUT | `/api/shares/{share_name}/dataobject/revoke` | Remove data objects | Body: `AddDataObjectsRequest` |
| PUT | `/api/shares/{share_name}/recipients/add` | Add recipient to share | `recipient_name` |
| PUT | `/api/shares/{share_name}/recipients/remove` | Remove recipient from share | `recipient_name` |

**AddDataObjectsRequest Body:**
```json
{
  "tables": ["catalog.schema.table1", "catalog.schema.table2"],
  "views": ["catalog.schema.view1"],
  "schemas": ["catalog.schema_name"]
}
```

### 3.3 Recipients (10 endpoints)

| Method | Endpoint | Description | Notes |
|--------|----------|-------------|-------|
| GET | `/api/recipients` | List all recipients | Query: `prefix`, `page_size` |
| GET | `/api/recipients/{recipient_name}` | Get recipient details | - |
| POST | `/api/recipients/d2d/{recipient_name}` | Create D2D recipient | Requires `recipient_identifier` |
| POST | `/api/recipients/d2o/{recipient_name}` | Create D2O recipient | Returns activation URL |
| DELETE | `/api/recipients/{recipient_name}` | Delete recipient | Owner only |
| PUT | `/api/recipients/{recipient_name}/tokens/rotate` | Rotate token | D2O only |
| PUT | `/api/recipients/{recipient_name}/ipaddress/add` | Add IP addresses | D2O only |
| PUT | `/api/recipients/{recipient_name}/ipaddress/revoke` | Revoke IP addresses | D2O only |
| PUT | `/api/recipients/{recipient_name}/description/update` | Update description | Owner only |
| PUT | `/api/recipients/{recipient_name}/expiration_time/update` | Update expiration | D2O only |

**D2D vs D2O Recipients:**

| Feature | D2D (Databricks-to-Databricks) | D2O (Databricks-to-Open) |
|---------|-------------------------------|--------------------------|
| Auth Type | `DATABRICKS` | `TOKEN` |
| Identifier | `cloud:region:uuid` | Auto-generated |
| IP Access Lists | Not supported | Supported |
| Token Rotation | Not supported | Supported |
| Expiration Time | Not supported | Supported |
| Use Case | Partner Databricks workspaces | Non-Databricks clients |

### 3.4 Catalogs (3 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/catalogs` | List all Unity Catalogs |
| GET | `/api/catalogs/{catalog_name}` | Get catalog details |
| POST | `/api/catalogs/{catalog_name}` | Create new catalog |

**Create Catalog Body (optional):**
```json
{
  "comment": "Catalog description",
  "external_location": "external_location_name"
}
```

### 3.5 Pipelines (10 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/pipelines` | List all DLT pipelines |
| GET | `/api/pipelines/{pipeline_name}` | Get pipeline by name |
| POST | `/api/pipelines/{pipeline_name}` | Create new pipeline |
| DELETE | `/api/pipelines/{pipeline_name}` | Delete pipeline |
| PUT | `/api/pipelines/{pipeline_name}/configuration` | Update pipeline config |
| PUT | `/api/pipelines/{pipeline_name}/libraries` | Update libraries/root path |
| PUT | `/api/pipelines/{pipeline_name}/notifications/add` | Add notification recipients |
| PUT | `/api/pipelines/{pipeline_name}/notifications/remove` | Remove notification recipients |
| PUT | `/api/pipelines/{pipeline_name}/continuous` | Toggle continuous mode |
| POST | `/api/pipelines/{pipeline_name}/full-refresh` | Trigger full refresh |

**Create Pipeline Request:**
```json
{
  "target_catalog_name": "dltshr_dev",
  "target_schema_name": "02_silver",
  "configuration": {
    "pipelines.source_table": "dltshr_dev.01_bronze.source_table",
    "pipelines.keys": "id,timestamp",
    "pipelines.target_table": "target_table_scd",
    "pipelines.scd_type": "2"
  },
  "notifications_list": ["user@example.com", "data-team-group"],
  "tags": {"environment": "dev", "owner": "data-team"},
  "serverless": true
}
```

**Pipeline Configuration Fields:**

| Field | Description | Required |
|-------|-------------|----------|
| `pipelines.source_table` | Source table (catalog.schema.table) | Yes |
| `pipelines.keys` | Primary key columns (comma-separated) | Yes |
| `pipelines.target_table` | Target table name | Yes |
| `pipelines.scd_type` | SCD type ("1" or "2") | Yes |
| `pipelines.sequence_by` | Sequence column (auto: `_commit_version`) | No |
| `pipelines.delete_expr` | Delete expression (auto: `_change_type = 'delete'`) | No |

### 3.6 Schedules (7 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/schedules` | List all schedules |
| GET | `/api/schedules/pipeline/{pipeline_name}` | List schedules for pipeline |
| POST | `/api/pipelines/{pipeline_name}/schedules` | Create schedule |
| PATCH | `/api/pipelines/{pipeline_name}/schedules/{job_name}/cron` | Update cron expression |
| PATCH | `/api/pipelines/{pipeline_name}/schedules/{job_name}/timezone` | Update timezone |
| DELETE | `/api/pipelines/{pipeline_name}/schedules/{job_name}` | Delete specific schedule |
| DELETE | `/api/pipelines/{pipeline_name}/schedules` | Delete all pipeline schedules |

**Create Schedule Request:**
```json
{
  "cron_expression": "0 0 12 * * ?",
  "time_zone": "America/New_York"
}
```

**Quartz Cron Format (6 fields):**
```
┌───────────── second (0-59)
│ ┌───────────── minute (0-59)
│ │ ┌───────────── hour (0-23)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12)
│ │ │ │ │ ┌───────────── day of week (1-7 or SUN-SAT)
│ │ │ │ │ │
0 0 12 * * ?    = Daily at noon
0 */15 * * * ?  = Every 15 minutes
0 30 9 ? * MON-FRI = Weekdays at 9:30 AM
```

### 3.7 Metrics (4 endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/pipelines/{pipeline_name}/metrics` | Get pipeline run metrics |
| GET | `/api/pipelines/{pipeline_name}/metrics?start_timestamp=...` | Filtered pipeline metrics |
| GET | `/api/pipelines/{pipeline_name}/job-runs/metrics` | Get job run metrics |
| GET | `/api/pipelines/{pipeline_name}/job-runs/metrics?start_timestamp=...` | Filtered job metrics |

**Metrics Response Fields:**
- `pipeline_id`, `pipeline_name`, `total_runs`
- Per run: `run_id`, `status`, `start_time`, `end_time`, `duration_seconds`, `error_message`

### 3.8 Workflow (4 endpoints) - Feature-Flagged

Requires `enable_workflow=true` in configuration.

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/workflow/sharepack/upload_and_validate` | Upload share pack (YAML/Excel) |
| GET | `/api/workflow/sharepack/{share_pack_id}` | Get provisioning status |
| GET | `/api/workflow/health` | Workflow system health check |

---

## 4. Authentication & Security

### 4.1 Authentication Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│ Azure APIM  │────▶│  FastAPI    │────▶│ Databricks  │
│             │     │             │     │             │     │ Workspace   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │                   │
       │ Subscription-Key  │ Validate key      │ X-Workspace-URL   │
       │ X-Workspace-URL   │                   │ OAuth2 token      │
       └───────────────────┴───────────────────┴───────────────────┘
```

### 4.2 Required Headers

| Header | Description | Required |
|--------|-------------|----------|
| `X-Workspace-URL` | Databricks workspace URL | Yes |
| `Subscription-Key` | API subscription key | Yes |

### 4.3 Workspace URL Validation

The `X-Workspace-URL` header is validated for:
1. **Protocol**: Must be HTTPS
2. **Domain Pattern**: Must match valid Databricks patterns:
   - Azure: `*.azuredatabricks.net`
   - AWS: `*.cloud.databricks.com`
   - GCP: `*.gcp.databricks.com`
3. **Reachability**: DNS resolution + HTTP HEAD request

### 4.4 Token Generation

OAuth2 Client Credentials flow via Azure Service Principal:

```python
# Token endpoint
https://accounts.azuredatabricks.net/oidc/accounts/{account_id}/v1/token

# Required credentials (from environment)
client_id     # Azure Service Principal Client ID
client_secret # Azure Service Principal Secret
account_id    # Databricks Account ID
```

**Token Caching:**
- Tokens cached in-memory (thread-safe `TokenManager`)
- 5-minute refresh buffer before expiry
- Automatic regeneration when expired

---

## 5. Configuration

### 5.1 Required Environment Variables

```env
# Databricks Authentication (REQUIRED)
client_id=<azure-service-principal-client-id>
client_secret=<azure-service-principal-secret>
account_id=<databricks-account-id>
```

### 5.2 Optional Environment Variables

```env
# Workspace URL (reference only - actual URL from X-Workspace-URL header)
dltshr_workspace_url=https://adb-xxx.azuredatabricks.net

# Azure Blob Logging
enable_blob_logging=false
azure_storage_account_url=https://<account>.blob.core.windows.net
azure_storage_sas_url=https://<account>.blob.core.windows.net/<container>?<sas>
azure_storage_logs_container=logging

# PostgreSQL Logging
enable_postgresql_logging=false
postgresql_connection_string=<connection-string>
postgresql_log_table=application_logs
postgresql_min_log_level=WARNING

# Datadog Logging
enable_datadog_logging=true
dd_api_key=<datadog-api-key>
dd_service=deltashare-api

# Workflow System
enable_workflow=false
domain_db_connection_string=<postgres-connection-string>
azure_queue_connection_string=<azure-queue-connection>
azure_queue_name=sharepack-processing
```

### 5.3 Configuration Loading Strategy

The application auto-detects the configuration source:

| Environment | Detection | Config Source |
|-------------|-----------|---------------|
| Azure Web App | `WEBSITE_INSTANCE_ID` exists | App Settings |
| Local (with .env) | `.env` file exists | `.env` file |
| Local (no .env) | Neither | Shell environment |

---

## 6. Business Logic Modules

### 6.1 Share Operations (`dltshr/share.py`)

| Function | Description | Returns |
|----------|-------------|---------|
| `list_shares_all()` | List all shares with optional prefix | `List[ShareInfo]` |
| `get_shares()` | Get single share by name | `ShareInfo \| None` |
| `create_share()` | Create new share | `ShareInfo \| str` |
| `delete_share()` | Delete share (owner-only) | `bool \| str` |
| `add_data_object_to_share()` | Add TABLE/VIEW/SCHEMA objects | `ShareInfo \| str` |
| `revoke_data_object_from_share()` | Remove objects | `ShareInfo \| str` |
| `add_recipients_to_share()` | Grant recipient access | `UpdateSharePermissionsResponse` |
| `remove_recipients_from_share()` | Revoke recipient access | `UpdateSharePermissionsResponse` |

### 6.2 Recipient Operations (`dltshr/recipient.py`)

| Function | Description | Returns |
|----------|-------------|---------|
| `list_recipients()` | List all recipients | `List[RecipientInfo]` |
| `get_recipients()` | Get single recipient | `RecipientInfo \| None` |
| `create_recipient_d2d()` | Create DATABRICKS-auth recipient | `RecipientInfo \| str` |
| `create_recipient_d2o()` | Create TOKEN-auth recipient | `RecipientInfo \| str` |
| `delete_recipient()` | Delete recipient | `bool \| str` |
| `add_recipient_ip()` | Add IP to D2O recipient | `RecipientInfo \| str` |
| `revoke_recipient_ip()` | Remove IP from D2O recipient | `RecipientInfo \| str` |
| `rotate_recipient_token()` | Generate new token for D2O | `RecipientInfo \| str` |

### 6.3 Pipeline Operations (`jobs/dbrx_pipelines.py`)

| Function | Description |
|----------|-------------|
| `list_pipelines()` | List DLT pipelines |
| `get_pipeline_by_name()` | Get single pipeline |
| `create_pipeline()` | Create DLT pipeline with validations |
| `delete_pipeline()` | Delete by pipeline_id |
| `update_pipeline_continuous()` | Enable/disable continuous mode |
| `pipeline_full_refresh()` | Trigger full refresh |
| `validate_and_prepare_source_table()` | Validate source table and enable CDF |
| `validate_pipeline_keys()` | Validate keys exist in source table |

**Pipeline Creation Validations:**
1. Target catalog exists (creates if needed)
2. Target schema exists (creates if needed)
3. Source table exists and has CDF enabled (enables if needed)
4. Pipeline keys are valid columns in source table

### 6.4 SDK Usage Pattern

```python
from databricks.sdk import WorkspaceClient
from dbrx_api.dbrx_auth.token_gen import get_auth_token
from datetime import datetime, timezone

# Get token
session_token = get_auth_token(datetime.now(timezone.utc))[0]

# Create client
w_client = WorkspaceClient(host=workspace_url, token=session_token)

# Use SDK methods
shares = w_client.shares.list_shares()
recipients = w_client.recipients.list()
pipelines = w_client.pipelines.list_pipelines()
```

---

## 7. Database & Storage

### 7.1 Workflow Domain Database (PostgreSQL)

The workflow system uses PostgreSQL with **SCD Type 2** pattern for full historical tracking.

**Schema:** `deltashare`

**SCD Type 2 Fields:**
| Field | Description |
|-------|-------------|
| `record_id` | Surrogate key (UUID) |
| `{entity}_id` | Business key (stable across versions) |
| `effective_from` | Version start timestamp |
| `effective_to` | Version end timestamp |
| `is_current` | Latest version flag |
| `is_deleted` | Soft delete flag |
| `version` | Sequential version number |

**Tables (16 total):**

**SCD2 Mutable Entities (11):**
- `tenants` - Business lines/organizations
- `tenant_regions` - Tenant-to-region-to-workspace mapping
- `projects` - Projects within tenants
- `requests` - Provisioning requests
- `share_packs` - Share pack configurations
- `recipients` - Delta Share recipients
- `shares` - Delta Shares
- `pipelines` - DLT pipelines
- `users` - Azure AD synced users
- `ad_groups` - Azure AD synced groups
- `databricks_objects` - Shared tables/views/schemas

**Append-Only Event Logs (5):**
- `job_metrics` - Pipeline execution metrics
- `project_costs` - Cost tracking
- `sync_jobs` - Azure AD sync history
- `notifications` - Event notifications
- `audit_trail` - All changes audit log

---

## 8. Monitoring & Logging

### 8.1 Logging Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Loguru Logger                          │
├─────────────────────────────────────────────────────────────┤
│  Console Sink  │  Blob Sink  │  PostgreSQL  │   Datadog    │
│   (always)     │  (optional) │   (optional)  │  (optional)  │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Log Sinks

| Sink | Enable Setting | Min Level | Use Case |
|------|----------------|-----------|----------|
| Console (stdout) | Always enabled | DEBUG | Development |
| Azure Blob Storage | `enable_blob_logging=true` | DEBUG | Persistent storage |
| PostgreSQL | `enable_postgresql_logging=true` | WARNING+ | Critical logs |
| Datadog | `enable_datadog_logging=true` | DEBUG | APM & monitoring |

### 8.3 Request Context Middleware

Captures per-request:
- Request ID (UUID)
- Client IP
- User identity
- User Agent
- Request path
- Request body (JSON-safe)
- Response status code
- Duration

### 8.4 Error Handling

| Error Type | HTTP Status | Description |
|------------|-------------|-------------|
| Pydantic validation | 422 | Invalid request data |
| Unauthenticated | 401 | Invalid credentials |
| PermissionDenied | 403 | Insufficient privileges |
| NotFound | 404 | Resource not found |
| BadRequest | 400 | Invalid parameters |
| DatabricksError | 502 | Upstream service error |
| Unhandled exception | 500 | Internal server error |

---

## 9. Testing

### 9.1 Test Structure

```
tests/
├── conftest.py                  # Pytest configuration
├── fixtures/                    # Test fixtures & mocks
│   ├── app_fixtures.py          # FastAPI app + test client
│   ├── databricks_fixtures.py   # Databricks SDK mocks
│   ├── pipeline_fixtures.py     # Pipeline test data
│   └── ...
└── unit_tests/                  # 30+ test files
    ├── test_routes_share.py
    ├── test_routes_recipient.py
    ├── test_routes_pipelines.py
    └── ...
```

### 9.2 Running Tests

```bash
# All tests with coverage
make test

# Quick tests (exclude slow tests)
make test-quick

# Specific test file
bash run.sh run-tests tests/unit_tests/test_routes_share.py

# Specific test function
bash run.sh run-tests tests/unit_tests/test_routes_share.py::test_list_shares

# View coverage report in browser
make serve-coverage-report
```

### 9.3 Test Fixtures

| Fixture | Description |
|---------|-------------|
| `mock_settings` | Settings with test configuration |
| `mock_token_manager` | Stubbed token generation |
| `app` | Full FastAPI app instance |
| `client` | TestClient with auth headers |
| `unauthenticated_client` | TestClient without headers |

---

## 10. Development Guide

### 10.1 Local Setup

```bash
# Clone repository
git clone <repo-url>
cd JLLT-EDP-DeltaShare/api_layer

# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
make install

# Create .env file
cat > .env << EOF
client_id=<azure-sp-client-id>
client_secret=<azure-sp-secret>
account_id=<databricks-account-id>
EOF

# Run development server
make run-dev

# API available at http://localhost:8000
```

### 10.2 Development Commands

```bash
make install           # Install dependencies
make run-dev          # Run development server (auto-reload)
make test             # Run all tests with coverage
make test-quick       # Run non-slow tests
make lint             # Run code formatting & linting
make clean            # Clean build artifacts
make build            # Build wheel + sdist
make serve-coverage-report  # View coverage in browser
```

### 10.3 Code Quality Standards

| Tool | Purpose | Configuration |
|------|---------|---------------|
| Black | Code formatting | Line length: 119 |
| isort | Import sorting | VERTICAL_HANGING_INDENT |
| pylint | Linting | Custom rules in pyproject.toml |
| flake8 | Style checking | With docstring checks |
| mypy | Type checking | Strict mode |

### 10.4 Pre-commit Hooks

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

Hooks include:
- Trailing whitespace cleanup
- End-of-file fixer
- Merge conflict detection
- Black formatting
- isort import sorting
- pylint linting

---

## 11. Deployment

### 11.1 Azure Web App Deployment

The application is deployed as an Azure Web App:

1. **Configuration**: Set environment variables in **Configuration > Application settings**
2. **Health Check**: Configure `/api/health` endpoint
3. **Auto-Detection**: App detects Azure via `WEBSITE_INSTANCE_ID` env var

### 11.2 CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/edp_apilayer.yml`):

```yaml
Triggers:
  - Push to develop/master branches
  - Changes in api_layer/ directory

Steps:
  1. Checkout code
  2. Install dependencies
  3. Run linting (make lint)
  4. Run tests (make test)
  5. Security scan (Snyk)
  6. Build package
  7. Deploy to Azure
```

### 11.3 Environment-Specific Configuration

| Environment | Base URL | Configuration |
|-------------|----------|---------------|
| Development | `https://api-dev.jll.com/udp/dltshr` | App Settings |
| UAT | `https://api-uat.jll.com/udp/dltshr` | App Settings |
| Production | `https://api.jll.com/udp/dltshr` | App Settings |

---

## 12. Workflow System

### 12.1 Overview

The workflow system enables bulk provisioning via YAML or Excel files:

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Upload  │────▶│ Validate │────▶│  Queue   │────▶│ Provision│
│  File    │     │ & Parse  │     │ Message  │     │ Resources│
└──────────┘     └──────────┘     └──────────┘     └──────────┘
                                                         │
                       ┌─────────────────────────────────┘
                       ▼
                 ┌──────────┐
                 │ Track in │
                 │ Database │
                 └──────────┘
```

### 12.2 Share Pack YAML Format

```yaml
metadata:
  version: "1.0"
  workspace_url: "https://adb-xxx.azuredatabricks.net"
  strategy: "NEW"  # NEW, UPDATE, or DELETE
  business_line: "Data Engineering"
  delta_share_region: "AM"  # AM or EMEA
  contact_email: "team@example.com"
  requestor: "user@example.com"
  approver: "manager@example.com"
  servicenow: "REQ123456"

recipient:
  - name: "partner_company"
    type: "D2D"
    data_recipient_global_metastore_id: "azure:eastus:uuid"
    description: "Partner company recipient"

  - name: "external_client"
    type: "D2O"
    description: "External client"
    ip_addresses:
      - "192.168.1.0/24"
      - "10.0.0.100"

share:
  - name: "analytics_share"
    description: "Shared analytics data"
    objects:
      - name: "catalog.schema.sales_table"
        type: "TABLE"
      - name: "catalog.schema.customer_view"
        type: "VIEW"
    recipients:
      - name: "partner_company"
      - name: "external_client"

pipeline:
  - name: "sales_pipeline"
    source_table: "catalog.bronze.raw_sales"
    keys: ["sale_id", "timestamp"]
    target_catalog: "catalog"
    target_schema: "silver"
    target_table: "processed_sales"
    scd_type: "2"
    notifications:
      - "team@example.com"
```

### 12.3 Provisioning Strategies

| Strategy | Description |
|----------|-------------|
| NEW | Create all resources from scratch |
| UPDATE | Update existing resources, create missing ones |
| DELETE | Name-only: list of recipients and shares. Unschedule (delete schedule) pipelines for each share; if pipelines are explicitly listed, unschedule and delete those pipelines. Then delete shares and recipients. |

---

## 13. Troubleshooting

### 13.1 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| 401 Unauthorized | Invalid subscription key | Verify `Subscription-Key` header |
| 400 Invalid workspace URL | Malformed URL | Use format: `https://adb-xxx.azuredatabricks.net` |
| 502 Workspace unreachable | Network/DNS issue | Check workspace URL is accessible |
| 403 Permission denied | Insufficient privileges | Verify service principal permissions |
| 404 Resource not found | Share/recipient doesn't exist | Check resource name spelling |

### 13.2 Debug Logging

Enable debug logging:
```env
# In .env or App Settings
LOG_LEVEL=DEBUG
```

### 13.3 Health Check Endpoints

```bash
# Basic health check
curl https://api-dev.jll.com/udp/dltshr/api/health

# Test logging connectivity
curl -X POST https://api-dev.jll.com/udp/dltshr/api/health/logging/test \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -H "Subscription-Key: your-key"

# Workflow health (if enabled)
curl https://api-dev.jll.com/udp/dltshr/api/workflow/health \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -H "Subscription-Key: your-key"
```

### 13.4 Postman Collections

Pre-built Postman collections are available in `/api_layer/postman_collection/`:

| Collection | Endpoints |
|------------|-----------|
| Health_API.postman_collection.json | 2 |
| Shares_API.postman_collection.json | 8 |
| Recipients_API.postman_collection.json | 10 |
| Catalog_API.postman_collection.json | 3 |
| Pipelines_API.postman_collection.json | 10 |
| Schedule_API.postman_collection.json | 7 |
| Metrics_API.postman_collection.json | 4 |
| Workflow_API.postman_collection.json | 4 |

**Total: 48 endpoints across 8 collections**

---

## Quick Reference

### API Base URLs

| Environment | URL |
|-------------|-----|
| Development | `https://api-dev.jll.com/udp/dltshr` |
| Swagger UI | `https://api-dev.jll.com/udp/dltshr/` |
| ReDoc | `https://api-dev.jll.com/udp/dltshr/redoc` |
| OpenAPI JSON | `https://api-dev.jll.com/udp/dltshr/openapi.json` |

### Required Headers

```http
X-Workspace-URL: https://adb-xxx.azuredatabricks.net
Subscription-Key: your-subscription-key-here
Content-Type: application/json
```

### Common curl Examples

```bash
# List shares
curl -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
     -H "Subscription-Key: your-key" \
     https://api-dev.jll.com/udp/dltshr/api/shares

# Create share
curl -X POST \
     -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
     -H "Subscription-Key: your-key" \
     "https://api-dev.jll.com/udp/dltshr/api/shares/my_share?description=Test%20share"

# Create D2O recipient
curl -X POST \
     -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
     -H "Subscription-Key: your-key" \
     "https://api-dev.jll.com/udp/dltshr/api/recipients/d2o/partner?description=Partner%20recipient"

# Create pipeline
curl -X POST \
     -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
     -H "Subscription-Key: your-key" \
     -H "Content-Type: application/json" \
     -d '{"target_catalog_name":"dltshr_dev","target_schema_name":"silver","configuration":{"pipelines.source_table":"dltshr_dev.bronze.source","pipelines.keys":"id","pipelines.target_table":"target","pipelines.scd_type":"1"},"serverless":true}' \
     https://api-dev.jll.com/udp/dltshr/api/pipelines/my_pipeline
```

---

**Documentation Version:** 1.0
**Last Updated:** 2024-02-07
**API Version:** 0.0.1
