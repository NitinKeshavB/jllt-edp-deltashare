# SharePack Excel Template

## Overview
This template shows how to create a SharePack configuration in Excel format (.xlsx).

**File Structure**: 4 sheets (metadata, recipient, share, pipelines)

---

## Sheet 1: `metadata`

**Format**: Two columns (Field | Value)

### Copy this data:

```
Field                   Value
requestor              john.doe@company.com
business_line          Finance Analytics
project_name           Q1 2025 Finance Reporting
strategy               NEW
delta_share_region     AM
configurator           data-engineering@company.com
approver               finance-leadership@company.com
executive_team         data-governance@company.com
approver_status        approved
workspace_url          https://adb-1234567890123456.12.azuredatabricks.net
servicenow             INC0012345
version                1.0
contact_email          john.doe@company.com
description            Finance analytics data sharing for Q1 2025
```

### Field Descriptions:
- **requestor**: Email or AD group name (REQUIRED)
- **business_line**: Business unit (REQUIRED)
- **project_name**: Project identifier for deduplication (OPTIONAL but recommended)
- **strategy**: NEW or UPDATE (REQUIRED)
- **delta_share_region**: AM or EMEA (REQUIRED)
- **configurator**: Email or AD group (REQUIRED)
- **approver**: Email or AD group (REQUIRED)
- **executive_team**: Email or AD group (REQUIRED)
- **approver_status**: approved | declined | request_more_info | pending (REQUIRED)
- **workspace_url**: Databricks workspace URL (REQUIRED, must be reachable)
- **servicenow**: ServiceNow ticket (REQUIRED)
- **version**: Version number (OPTIONAL)
- **contact_email**: Contact email (REQUIRED if different from requestor)
- **description**: Description (OPTIONAL)

---

## Sheet 2: `recipient`

**Columns**: name | type | recipient | recipient_databricks_org | recipient_ips_to_add | recipient_ips_to_remove | token_expiry | token_rotation | comment

### NEW Strategy Example:

```
name                        type    recipient                       recipient_databricks_org                        recipient_ips_to_add              recipient_ips_to_remove    token_expiry    token_rotation    comment
external_analytics_partner  D2O     partner@external-company.com                                                    203.0.113.0/24,198.51.100.50                                 90              false             External analytics partner for Q1
internal_analytics_team     D2D     analytics-team@company.com      aws:us-west-2:a1b2c3d4-e5f6-7890-abcd-ef123                                                                                                    Internal analytics workspace
```

### UPDATE Strategy Example (IP Management):

```
name                        type    recipient                       recipient_ips_to_add    recipient_ips_to_remove    comment
external_analytics_partner  D2O     partner@external-company.com    203.0.113.100           198.51.100.50              Updated IP allowlist
internal_analytics_team     D2D     analytics-team@company.com                                                         Internal analytics workspace
```

### Column Descriptions:
- **name**: Unique recipient identifier (REQUIRED)
- **type**: D2O or D2D (REQUIRED)
- **recipient**: Contact email (REQUIRED)
- **recipient_databricks_org**: Metastore ID - format: `cloud:region:uuid` (REQUIRED for D2D)
- **recipient_ips_to_add**: Comma-separated IPs or CIDR blocks (D2O only)
- **recipient_ips_to_remove**: Comma-separated IPs to remove (UPDATE strategy, D2O only)
- **token_expiry**: Days (D2O only, default: 90)
- **token_rotation**: true or false (D2O only)
- **comment**: Description (OPTIONAL)

---

## Sheet 3: `share`

**Columns**: name | comment | recipients | share_assets | ext_catalog_name | ext_schema_name | tags

### Example:

```
name                         comment                                    recipients                                            share_assets                                                                                                                          ext_catalog_name    ext_schema_name    tags
finance_daily_reports        Daily financial reporting data for Q1      external_analytics_partner,internal_analytics_team    main_catalog.finance_prod.daily_transactions,main_catalog.finance_prod.revenue_summary,main_catalog.finance_prod.customer_metrics   analytics_prod      finance_shared     production,finance,q1_2025
operations_realtime_metrics  Real-time operational metrics              internal_analytics_team                               ops_catalog.metrics_prod.system_health,ops_catalog.metrics_prod.error_logs,ops_catalog.metrics_prod.user_activity                    ops_analytics       realtime_metrics   production,operations,monitoring
```

### Column Descriptions:
- **name**: Share name (REQUIRED)
- **comment**: Description (OPTIONAL)
- **recipients**: Comma-separated recipient names (REQUIRED)
- **share_assets**: Comma-separated 3-part table names (REQUIRED)
- **ext_catalog_name**: Target catalog (REQUIRED)
- **ext_schema_name**: Target schema (REQUIRED)
- **tags**: Comma-separated tags (OPTIONAL)

---

## Sheet 4: `pipelines`

**Columns**: share_name | name_prefix | source_asset | target_asset | scd_type | key_columns | serverless | ext_catalog_name | ext_schema_name | notification | tags | schedule_action | schedule_cron | schedule_timezone | description

### NEW Strategy Example:

```
share_name                   name_prefix                     source_asset                                  target_asset                  scd_type    key_columns                              serverless    ext_catalog_name              ext_schema_name    notification                                    tags                                                            schedule_action    schedule_cron      schedule_timezone    description
finance_daily_reports        finance_transactions_daily      main_catalog.finance_prod.daily_transactions  daily_transactions_external   2           transaction_id,transaction_date          true                                                               finance-ops@company.com,data-quality@company.com environment:production;owner:finance_team;sla:daily                              0 0 2 * * ?         America/New_York     Daily transaction data sync with SCD Type 2
finance_daily_reports        finance_revenue_summary         main_catalog.finance_prod.revenue_summary     revenue_summary_external      1                                                    true                                                               finance-leadership@company.com                  environment:production;owner:finance_team                                       0 0 */6 * * ?       UTC                  Revenue summary sync every 6 hours
finance_daily_reports        finance_customer_metrics        main_catalog.finance_prod.customer_metrics    customer_metrics_v1           2           customer_id,metric_date                  true          customer_analytics_catalog    metrics_schema     customer-analytics@company.com                  environment:production;owner:customer_analytics_team;pii:true                   0 0 0 * * ?         America/Los_Angeles  Customer metrics with custom catalog
operations_realtime_metrics  ops_system_health_monitor       ops_catalog.metrics_prod.system_health        system_health_realtime        1                                                    true                                                               ops-team@company.com,sre-oncall@company.com    environment:production;monitoring:critical;alert_enabled:true                   0 */5 * * * ?       UTC                  System health monitoring every 5 minutes
operations_realtime_metrics  ops_error_logs_sync             ops_catalog.metrics_prod.error_logs           error_logs_archive            1                                                    true                                                               ops-team@company.com                            environment:production;log_type:errors;retention:90_days                        0 */15 * * * ?      UTC                  Error logs sync every 15 minutes
operations_realtime_metrics  ops_user_activity_tracking      ops_catalog.metrics_prod.user_activity        user_activity_analytics       2           user_id,activity_timestamp,session_id    true                                                               analytics-team@company.com                      environment:production;analytics:user_behavior;pii:true                         0 0 9-17 * * MON-FRI America/New_York    User activity tracking during business hours
```

### UPDATE Strategy Examples:

**Example 1: Update target_asset and schedule**
```
share_name             name_prefix                     source_asset                                  target_asset        scd_type    key_columns                                      serverless    schedule_cron    schedule_timezone
finance_daily_reports  finance_transactions_daily      main_catalog.finance_prod.daily_transactions  daily_txn_v2        2           transaction_id,transaction_date,customer_id      true          0 0 3 * * ?       America/New_York
```

**Example 2: Add new schedule**
```
share_name             name_prefix              source_asset                               target_asset                 scd_type    schedule_cron      schedule_timezone
finance_daily_reports  finance_revenue_summary  main_catalog.finance_prod.revenue_summary  revenue_summary_external     1           0 0 */12 * * ?     UTC
```

**Example 3: Remove schedule**
```
share_name                   name_prefix                source_asset                            target_asset              scd_type    schedule_action
operations_realtime_metrics  ops_system_health_monitor  ops_catalog.metrics_prod.system_health  system_health_realtime    1           remove
```

### Column Descriptions:
- **share_name**: Share this pipeline belongs to (REQUIRED, must match share name)
- **name_prefix**: Pipeline name (REQUIRED)
- **source_asset**: Source table - 3-part name (REQUIRED, IMMUTABLE in UPDATE)
- **target_asset**: Target table name only (REQUIRED, can update in UPDATE)
- **scd_type**: 1 or 2 (REQUIRED, IMMUTABLE in UPDATE)
- **key_columns**: Comma-separated column names (REQUIRED for Type 2, validated against source)
- **serverless**: true or false (OPTIONAL, can update in UPDATE)
- **ext_catalog_name**: Override catalog (OPTIONAL, overrides share-level)
- **ext_schema_name**: Override schema (OPTIONAL, overrides share-level)
- **notification**: Comma-separated email addresses (OPTIONAL, can update in UPDATE)
- **tags**: Semicolon-separated key:value pairs (OPTIONAL, can update in UPDATE)
  - Format: `key1:value1;key2:value2;key3:value3`
- **schedule_action**: "remove" to delete schedule (UPDATE only)
- **schedule_cron**: Quartz cron expression (OPTIONAL, can update in UPDATE)
- **schedule_timezone**: IANA timezone (OPTIONAL, can update in UPDATE)
- **description**: Pipeline description (OPTIONAL)

---

## Tags Format

### In pipelines sheet, use semicolon-separated key:value pairs:

```
environment:production;owner:finance_team;sla:daily;data_classification:confidential
```

**Example tags**:
- `environment:production`
- `owner:finance_team`
- `sla:daily`
- `data_classification:confidential`
- `pii:true`
- `monitoring:critical`
- `retention:90_days`

---

## Schedule Cron Format

Quartz 6-field format:
```
┌────── second (0-59)
│ ┌──── minute (0-59)
│ │ ┌── hour (0-23)
│ │ │ ┌── day of month (1-31)
│ │ │ │ ┌── month (1-12)
│ │ │ │ │ ┌── day of week (0-6, 0=Sunday)
│ │ │ │ │ │
* * * * * *
```

### Common Examples:
- `0 0 0 * * ?` - Daily at midnight
- `0 0 2 * * ?` - Daily at 2 AM
- `0 0 */6 * * ?` - Every 6 hours
- `0 */15 * * * ?` - Every 15 minutes
- `0 */5 * * * ?` - Every 5 minutes
- `0 0 9 ? * MON` - Every Monday at 9 AM
- `0 0 9 ? * MON-FRI` - Weekdays at 9 AM
- `0 0 9-17 * * MON-FRI` - Business hours (9 AM - 5 PM, Mon-Fri)
- `0 0 0 1 * ?` - First day of each month
- `continuous` - Real-time streaming (not yet supported)

---

## How to Create the Excel File

1. **Create new Excel workbook**
2. **Create 4 sheets**: `metadata`, `recipient`, `share`, `pipelines`
3. **Copy data from above sections** into respective sheets
4. **Format as table** (optional but recommended):
   - Select data range
   - Insert → Table
   - Check "My table has headers"
5. **Save as** `.xlsx` format
6. **Upload** via `/workflow/sharepack/upload_and_validate` endpoint

---

## Validation Rules

### Metadata
- ✅ Valid email formats or AD group names
- ✅ workspace_url must be HTTPS and reachable
- ✅ servicenow is required
- ✅ strategy must be "NEW" or "UPDATE"

### Recipients
- ✅ **NEW**: Can create new recipients
- ❌ **UPDATE**: Recipients must already exist
- ✅ D2O: Supports IP allowlist (optional)
- ✅ D2D: Requires recipient_databricks_org

### Shares
- ✅ **NEW**: Can create new shares
- ❌ **UPDATE**: Shares must already exist
- ✅ Every share_asset must have a corresponding pipeline

### Pipelines
- ✅ **NEW**: Can create new pipelines
- ❌ **UPDATE**: Pipelines must already exist
- ❌ **UPDATE**: Cannot change source_asset (IMMUTABLE)
- ❌ **UPDATE**: Cannot change scd_type (IMMUTABLE)
- ✅ **UPDATE**: Can change target_asset, key_columns (validated), serverless, notifications, tags, schedule
- ✅ key_columns validated against source table schema
- ✅ Schedule operations: add, update, remove

---

## Error Handling

### Non-Retryable (Fail Immediately)
- Validation errors
- Immutable field changes
- Invalid key_columns
- Resource doesn't exist (UPDATE)

**Result**: Status = FAILED, no retry

### Retryable (Retry Once After 10 Minutes)
- Timeout errors
- Connection errors
- HTTP 503/504

**Result**: Retry → If fails → Status = FAILED with "Retried failed request and stopping"

---

## Tips

1. **Use Excel tables** - Makes it easier to manage data
2. **Freeze header rows** - View → Freeze Panes
3. **Validate data** - Check email formats, URLs before uploading
4. **Test with NEW first** - Create resources, then use UPDATE
5. **Keep backup** - Save copies before making changes
6. **Check logs** - Review validation errors from API response

---

## Example File Download

Reference the existing file at:
`/home/nitinkeshav/JLLT-EDP-DELTASHARE/api_layer/sample_sharepack.xlsx`

This template provides the exact structure needed for upload.
