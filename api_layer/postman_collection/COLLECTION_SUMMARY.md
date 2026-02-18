# Delta Share API - Postman Collections Summary

## Overview
This directory contains comprehensive Postman Collection v2.1.0 files for all Delta Share API endpoints. All collections use `https://api-dev.jll.com/udp/dltshr` as the default base URL.

## Collections Inventory

### 1. **Health_API.postman_collection.json** (2 endpoints)
System health checks and logging tests.

**Endpoints:**
- `GET /health` - Get health status (service version, timestamp, connection status)
- `POST /health/logging/test` - Test logging system (Azure Blob, PostgreSQL, Datadog)

---

### 2. **Shares_API.postman_collection.json** (8 endpoints)
Share creation and management with data objects and recipient permissions.

**Endpoints:**
- `GET /shares` - List all shares (with prefix filtering and pagination)
- `GET /shares/{share_name}` - Get share by name
- `POST /shares/{share_name}` - Create share
- `DELETE /shares/{share_name}` - Delete share
- `PUT /shares/{share_name}/dataobject/add` - Add data objects (tables/views/schemas)
- `PUT /shares/{share_name}/dataobject/revoke` - Remove data objects
- `PUT /shares/{share_name}/recipients/add` - Add recipient to share
- `PUT /shares/{share_name}/recipients/remove` - Remove recipient from share

---

### 3. **Recipients_API.postman_collection.json** (10 endpoints)
Complete recipient management for D2D (Databricks-to-Databricks) and D2O (Databricks-to-Open) sharing.

**Endpoints:**
- `GET /recipients` - List all recipients (with prefix filtering)
- `GET /recipients/{recipient_name}` - Get recipient by name
- `POST /recipients/d2d/{recipient_name}` - Create D2D recipient (DATABRICKS auth)
- `POST /recipients/d2o/{recipient_name}` - Create D2O recipient (TOKEN auth)
- `DELETE /recipients/{recipient_name}` - Delete recipient
- `PUT /recipients/{recipient_name}/tokens/rotate` - Rotate recipient token (D2O only)
- `PUT /recipients/{recipient_name}/ipaddress/add` - Add IP addresses (D2O only)
- `PUT /recipients/{recipient_name}/ipaddress/revoke` - Revoke IP addresses (D2O only)
- `PUT /recipients/{recipient_name}/description/update` - Update description
- `PUT /recipients/{recipient_name}/expiration_time/update` - Update expiration time (D2O only)

---

### 4. **Catalog_API.postman_collection.json** (3 endpoints)
Unity Catalog management for creating and managing catalogs.

**Endpoints:**
- `GET /catalogs` - List all catalogs
- `GET /catalogs/{catalog_name}` - Get catalog details
- `POST /catalogs/{catalog_name}` - Create catalog

---

### 5. **Pipelines_API.postman_collection.json** (10 endpoints)
Delta Live Tables (DLT) pipeline creation and management with comprehensive configuration options.

**Endpoints:**
- `GET /pipelines` - List all pipelines (with search filtering)
- `GET /pipelines/{pipeline_name}` - Get pipeline by name
- `POST /pipelines/{pipeline_name}` - Create pipeline (with pre-creation validations)
- `DELETE /pipelines/{pipeline_name}` - Delete pipeline
- `PUT /pipelines/{pipeline_name}/configuration` - Update pipeline configuration (keys, target_table)
- `PUT /pipelines/{pipeline_name}/libraries` - Update pipeline libraries/root path
- `PUT /pipelines/{pipeline_name}/notifications/add` - Add notification recipients
- `PUT /pipelines/{pipeline_name}/notifications/remove` - Remove notification recipients
- `PUT /pipelines/{pipeline_name}/continuous` - Update continuous mode (on/off)
- `POST /pipelines/{pipeline_name}/full-refresh` - Perform full refresh

---

### 6. **Schedule_API.postman_collection.json** (7 endpoints)
Manage Databricks job schedules for pipeline automation using Quartz cron expressions.

**Endpoints:**
- `GET /schedules` - List all schedules (auto-paginated)
- `GET /schedules/pipeline/{pipeline_name}` - List schedules for specific pipeline
- `POST /pipelines/{pipeline_name}/schedules` - Create schedule
- `PATCH /pipelines/{pipeline_name}/schedules/{job_name}/cron` - Update cron expression
- `PATCH /pipelines/{pipeline_name}/schedules/{job_name}/timezone` - Update timezone
- `DELETE /pipelines/{pipeline_name}/schedules/{job_name}` - Delete specific schedule
- `DELETE /pipelines/{pipeline_name}/schedules` - Delete all schedules for pipeline

**Cron Examples (Quartz format - 6 fields):**
- Daily at noon: `0 0 12 * * ?`
- Every 15 minutes: `0 */15 * * * ?`
- Weekdays at 9:30 AM: `0 30 9 ? * MON-FRI`

---

### 7. **Metrics_API.postman_collection.json** (4 endpoints)
Extract comprehensive metrics for pipeline and job runs with optional timestamp filtering.

**Endpoints:**
- `GET /pipelines/{pipeline_name}/metrics` - Get pipeline run metrics
- `GET /pipelines/{pipeline_name}/metrics?start_timestamp=...` - Pipeline metrics (filtered)
- `GET /pipelines/{pipeline_name}/job-runs/metrics` - Get job run metrics
- `GET /pipelines/{pipeline_name}/job-runs/metrics?start_timestamp=...` - Job metrics (filtered)

---

### 8. **Workflow_API.postman_collection.json** (4 endpoints)
Share pack workflow management for bulk provisioning via YAML or Excel files. Requires `enable_workflow=true`.

**Endpoints:**
- `POST /workflow/sharepack/upload_and_validate` - Upload share pack (YAML)
- `POST /workflow/sharepack/upload_and_validate` - Upload share pack (Excel)
- `GET /workflow/sharepack/{share_pack_id}` - Get share pack status
- `GET /workflow/health` - Workflow system health check

---

## Common Collection Variables

All collections include these standard variables:

| Variable | Default Value | Description |
|----------|--------------|-------------|
| `base_url` | `https://api-dev.jll.com/udp/dltshr` | API base URL |
| `workspace_url` | `https://adb-1234567890123456.12.azuredatabricks.net` | Databricks workspace URL (sent via `X-Workspace-URL` header) |
| `subscription_key` | `your-subscription-key-here` | API subscription key |

Additional entity-specific variables (e.g., `share_name`, `recipient_name`, `pipeline_name`) are included in each collection.

## Standard Headers

All requests include:
- `X-Workspace-URL`: Databricks workspace URL (per-request specification)
- `Subscription-Key`: API subscription key

## Total API Coverage

| Metric | Value |
|--------|-------|
| **Total Collections** | 8 |
| **Total Endpoints** | 48 |
| **Collection Format** | Postman v2.1.0 |

## Import Instructions

1. Open Postman
2. Click **Import** button
3. Select **File** tab
4. Choose collection JSON file(s) - you can select all 8 at once
5. Click **Import**

## Configuration Steps

After importing:

1. **Update Collection Variables**:
   - `base_url`: Your API URL (default: `https://api-dev.jll.com/udp/dltshr`)
   - `workspace_url`: Your Databricks workspace URL
   - `subscription_key`: Your API subscription key

2. **Environment Setup** (Optional):
   - Create environments for DEV/UAT/PROD
   - Override `base_url` and `workspace_url` per environment

3. **Test Connection**:
   - Start with Health API collection
   - Run `GET /health` to verify connectivity

## Support

For API documentation, refer to the FastAPI interactive docs at:
- Swagger UI: `https://api-dev.jll.com/udp/dltshr/`
- ReDoc: `https://api-dev.jll.com/udp/dltshr/redoc`
