# Delta Share API - Postman Collections Summary

## Overview
This directory contains comprehensive Postman Collection v2.1.0 files for all Delta Share API endpoints. All collections have been updated with proper headers and variables for Azure deployment.

## Collections Inventory

### 1. **Recipients_API.postman_collection.json** (11 endpoints, 11KB)
Complete recipient management for D2D (Databricks-to-Databricks) and D2O (Databricks-to-Open) sharing.

**Endpoints:**
- `GET /recipients` - List all recipients (with prefix filtering)
- `GET /recipients/{recipient_name}` - Get recipient by name
- `POST /recipients/d2d/{recipient_name}` - Create D2D recipient
- `POST /recipients/d2o/{recipient_name}` - Create D2O recipient  
- `DELETE /recipients/{recipient_name}` - Delete recipient
- `PUT /recipients/{recipient_name}/tokens/rotate` - Rotate recipient token (D2O only)
- `PUT /recipients/{recipient_name}/ipaddress/add` - Add IP addresses (D2O only)
- `PUT /recipients/{recipient_name}/ipaddress/revoke` - Revoke IP addresses (D2O only)
- `PUT /recipients/{recipient_name}/description/update` - Update description
- `PUT /recipients/{recipient_name}/expiration_time/update` - Update expiration time (D2O only)

**Key Features:**
- D2D recipients: DATABRICKS authentication with metastore ID
- D2O recipients: TOKEN authentication with IP access lists
- Token rotation and expiration management
- IP address allow list management

---

### 2. **Pipelines_API.postman_collection.json** (12 endpoints, 15KB)
Delta Live Tables (DLT) pipeline creation and management with comprehensive configuration options.

**Endpoints:**
- `GET /pipelines` - List all pipelines (with search filtering)
- `GET /pipelines/{pipeline_name}` - Get pipeline by name
- `POST /pipelines/{pipeline_name}` - Create pipeline
- `DELETE /pipelines/{pipeline_name}` - Delete pipeline
- `PUT /pipelines/{pipeline_name}/configuration` - Update pipeline configuration (keys, target_table)
- `PUT /pipelines/{pipeline_name}/libraries` - Update pipeline libraries/root path
- `PUT /pipelines/{pipeline_name}/notifications/add` - Add notification recipients
- `PUT /pipelines/{pipeline_name}/notifications/remove` - Remove notification recipients
- `PUT /pipelines/{pipeline_name}/continuous` - Update continuous mode (on/off)
- `POST /pipelines/{pipeline_name}/full-refresh` - Perform full refresh

**Key Features:**
- SCD Type 1 and Type 2 support
- Notification management (emails and AD groups)
- Continuous vs triggered mode
- Full refresh capability
- Validated configurations with pre-creation checks

---

### 3. **Schedule_API.postman_collection.json** (7 endpoints, 7.4KB)
Manage Databricks job schedules for pipeline automation using Quartz cron expressions.

**Endpoints:**
- `GET /schedules` - List all schedules (auto-paginated)
- `GET /schedules/pipeline/{pipeline_name}` - List schedules for specific pipeline
- `POST /pipelines/{pipeline_name}/schedules` - Create schedule
- `PATCH /pipelines/{pipeline_name}/schedules/{job_name}/cron` - Update cron expression
- `PATCH /pipelines/{pipeline_name}/schedules/{job_name}/timezone` - Update timezone
- `DELETE /pipelines/{pipeline_name}/schedules/{job_name}` - Delete specific schedule
- `DELETE /pipelines/{pipeline_name}/schedules` - Delete all schedules for pipeline

**Key Features:**
- Quartz cron format (6 fields: sec min hour day-of-month month day-of-week)
- Timezone support (UTC, America/New_York, Europe/London, etc.)
- Email notifications
- Job tags for organization
- Auto-pagination for large result sets

**Cron Examples:**
- Daily at noon: `0 0 12 * * ?`
- Every 15 minutes: `0 */15 * * * ?`
- Weekdays at 9:30 AM: `0 30 9 ? * MON-FRI`

---

### 4. **Metrics_API.postman_collection.json** (4 endpoints, 4.5KB)
Extract comprehensive metrics for pipeline and job runs with optional timestamp filtering.

**Endpoints:**
- `GET /pipelines/{pipeline_name}/metrics` - Get pipeline run metrics
- `GET /pipelines/{pipeline_name}/metrics?start_timestamp=...` - Get pipeline run metrics (filtered)
- `GET /pipelines/{pipeline_name}/job-runs/metrics` - Get job run metrics
- `GET /pipelines/{pipeline_name}/job-runs/metrics?start_timestamp=...` - Get job run metrics (filtered)

**Metrics Included:**
- Pipeline information (ID, name, state, catalog, schema)
- Run details (update_id, status, full refresh flag, triggered by)
- Timing information (start time, end time, duration in seconds)
- Job schedule information (cron expression)
- Error messages for failed runs
- Result states (SUCCESS, FAILED, TERMINATED)

**Key Features:**
- ISO timestamp filtering (e.g., `2024-01-23T10:30:00Z`)
- Returns all runs if timestamp not provided
- Comprehensive run history
- Performance monitoring

---

### 5. **Workflow_API.postman_collection.json** (4 endpoints, 3.7KB)
Share pack workflow management for bulk provisioning via YAML or Excel files.

**Endpoints:**
- `POST /workflow/sharepack/upload_and_validate` - Upload share pack (YAML/Excel)
- `GET /workflow/sharepack/{share_pack_id}` - Get share pack status
- `GET /workflow/health` - Workflow system health check

**Key Features:**
- Async processing via Azure Storage Queue
- Smart strategy auto-detection
- Validation warnings and errors
- Database and queue health checks
- Supports both YAML and Excel formats

---

### 6. **Catalog_API.postman_collection.json** (3 endpoints, 2.9KB)
Unity Catalog management for creating and managing catalogs.

**Endpoints:**
- `POST /catalogs/{catalog_name}` - Create catalog
- `GET /catalogs/{catalog_name}` - Get catalog details
- `GET /catalogs` - List all catalogs

**Key Features:**
- Unity Catalog integration
- External location support
- Privilege management (CREATE CATALOG required)
- Service principal access control

---

### 7. **Shares_API.postman_collection.json** (8 endpoints, 7.0KB)
Share creation and management with data objects and recipient permissions.

**Endpoints:**
- `GET /shares` - List all shares (with prefix filtering)
- `GET /shares/{share_name}` - Get share by name
- `POST /shares/{share_name}` - Create share
- `DELETE /shares/{share_name}` - Delete share
- `PUT /shares/{share_name}/dataobject/add` - Add data objects (tables/views/schemas)
- `PUT /shares/{share_name}/dataobject/revoke` - Remove data objects
- `PUT /shares/{share_name}/recipients/add` - Add recipient to share
- `PUT /shares/{share_name}/recipients/remove` - Remove recipient from share

**Key Features:**
- Multi-object support (tables, views, schemas)
- Recipient permissions management
- Storage root configuration
- Share descriptions

---

### 8. **Health_API.postman_collection.json** (2 endpoints, 2.1KB)
System health checks and logging tests.

**Endpoints:**
- `GET /health` - Get health status
- `POST /health/logging/test` - Test logging system

**Key Features:**
- Service version and timestamp
- Connection status verification
- Logging system validation (Azure Blob, PostgreSQL, Datadog)

---

## Common Collection Variables

All collections include these standard variables:

```json
{
  "base_url": "https://your-api.azurewebsites.net",
  "workspace_url": "https://adb-xxxx.azuredatabricks.net",
  "subscription_key": "your-subscription-key-here"
}
```

Additional entity-specific variables are included in each collection.

## Standard Headers

All requests include:
- `X-Workspace-URL`: Databricks workspace URL (per-request specification)
- `Ocp-Apim-Subscription-Key`: Azure API Management subscription key

## Total API Coverage

- **Total Endpoints**: 48
- **Total Collections**: 8
- **File Size**: ~53KB total

## Import Instructions

1. Open Postman
2. Click **Import** button
3. Select **File** tab
4. Choose collection JSON file(s)
5. Click **Import**

## Configuration Steps

After importing:

1. **Update Collection Variables**:
   - `base_url`: Your Azure Web App URL
   - `workspace_url`: Your Databricks workspace URL  
   - `subscription_key`: Your Azure APIM subscription key

2. **Environment Setup** (Optional):
   - Create environments for DEV/TEST/PROD
   - Override base_url and workspace_url per environment

3. **Test Connection**:
   - Start with Health API collection
   - Run `GET /health` to verify connectivity

## Usage Examples

### Example 1: Create D2O Recipient with IP Restrictions
```
POST /recipients/d2o/external_partner
?description=External partner for analytics
&ip_access_list=203.0.113.0/24,198.51.100.10
```

### Example 2: Create Pipeline with Notifications
```json
POST /pipelines/my_pipeline
{
  "target_catalog_name": "analytics",
  "target_schema_name": "bronze",
  "configuration": {
    "pipelines.source_table": "raw.cdc.customers",
    "pipelines.keys": "customer_id",
    "pipelines.target_table": "customers_scd2",
    "pipelines.scd_type": "2"
  },
  "notifications_list": ["data-team@company.com"],
  "serverless": true
}
```

### Example 3: Schedule Daily Pipeline Run
```json
POST /pipelines/my_pipeline/schedules
{
  "job_name": "daily_customer_sync",
  "cron_expression": "0 0 2 * * ?",
  "time_zone": "America/New_York",
  "paused": false,
  "email_notifications": ["admin@company.com"]
}
```

## Notes

- All endpoints support proper error responses (400, 401, 403, 404, 409, 500)
- Timestamps use ISO 8601 format (`YYYY-MM-DDTHH:mm:ssZ`)
- IP addresses support both single IPs and CIDR blocks
- Cron expressions use Quartz format (6 required fields)
- All collections follow Postman Collection v2.1.0 specification

## Support

For API documentation, refer to the FastAPI interactive docs at:
- Swagger UI: `https://your-api.azurewebsites.net/`
- ReDoc: `https://your-api.azurewebsites.net/redoc`
