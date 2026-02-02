# SharePack Excel Template v2.0 - Structure Guide

This document describes the Excel template structure for SharePack provisioning with the new explicit source_asset format.

## Excel Workbook Structure

The Excel file should contain **4 sheets**:

1. **metadata** - Project and governance information
2. **recipient** - Recipient definitions (D2D and D2O)
3. **share** - Share definitions and assets
4. **pipelines** - Pipeline configurations with explicit source assets

---

## Sheet 1: metadata

| Column Name | Required | Example Value | Description |
|-------------|----------|---------------|-------------|
| requestor | Yes | test.user@jll.com | Email of person requesting the share pack |
| project_name | Yes | SharePack Demo - Q1 2024 | Human-readable project name |
| business_line | Yes | Data Platform Engineering | Business unit or team |
| strategy | Yes | NEW | Provisioning strategy: NEW or UPDATE |
| description | No | Demo SharePack | Project description |
| delta_share_region | Yes | AM | Region: AM or EMEA |
| configurator | Yes | data-platform-team@jll.com | Technical contact |
| approver | Yes | analytics-leadership@jll.com | Business approver |
| executive_team | Yes | data-governance-team@jll.com | Executive oversight |
| approver_status | Yes | approved | Status: approved, declined, request_more_info, pending |
| workspace_url | Yes | https://adb-123.12.azuredatabricks.net | Databricks workspace URL |
| version | No | 2.0 | Version number |
| contact_email | No | test.user@jll.com | Primary contact |

**Important:** Only one row of data (headers + 1 data row)

---

## Sheet 2: recipient

| Column Name | Required | Example Value | Description |
|-------------|----------|---------------|-------------|
| name | Yes | external_partner_recipient | Unique recipient identifier |
| type | Yes | D2O | Recipient type: D2O or D2D |
| recipient | Yes | partner@external-company.com | Contact email address |
| recipient_databricks_org | Conditional | aws:us-west-2:abc-123-def-456 | Metastore ID (REQUIRED for D2D, leave empty for D2O) |
| recipient_ips | Conditional | 203.0.113.0/24,198.51.100.50 | Comma-separated IP allowlist (D2O only) |
| token_expiry | No | 90 | Token expiry in days (D2O only) |
| token_rotation | No | false | Enable token rotation (D2O only) |
| comment | No | External partner for Q1 sales | Description |

**Multiple rows:** One row per recipient

**Example rows:**
```
name,type,recipient,recipient_databricks_org,recipient_ips,token_expiry,token_rotation,comment
external_partner_recipient,D2O,partner@external-company.com,,203.0.113.0/24,90,false,External partner
internal_analytics_team,D2D,analytics@jll.com,aws:us-west-2:abc-123-def,,,false,Internal team
```

---

## Sheet 3: share

| Column Name | Required | Example Value | Description |
|-------------|----------|---------------|-------------|
| share_name | Yes | sales_analytics_share | Unique share identifier |
| comment | No | Q1 sales analytics data | Share description |
| recipients | Yes | external_partner_recipient,internal_analytics_team | Comma-separated recipient names |
| share_assets | Yes | main_catalog.sales_schema.daily_sales,main_catalog.sales_schema.customer_orders | Comma-separated 3-part table names |
| ext_catalog_name | Yes | analytics_prod | Default target catalog for pipelines |
| ext_schema_name | Yes | shared_sales | Default target schema for pipelines |
| prefix_assetname | No | prod_ | Prefix for target table names |
| tags | No | production,sales_analytics | Comma-separated tags |

**Multiple rows:** One row per share

**Example rows:**
```
share_name,comment,recipients,share_assets,ext_catalog_name,ext_schema_name,prefix_assetname,tags
sales_analytics_share,Q1 sales data,external_partner_recipient,main_catalog.sales_schema.daily_sales,analytics_prod,shared_sales,prod_,production,sales
```

---

## Sheet 4: pipelines

| Column Name | Required | Example Value | Description |
|-------------|----------|---------------|-------------|
| share_name | Yes | sales_analytics_share | Which share this pipeline belongs to |
| name_prefix | Yes | sales_daily_sync | Pipeline name/identifier |
| **source_asset** | **Yes** | **main_catalog.sales_schema.daily_sales** | **EXPLICIT: Which share_asset this pipeline processes** |
| scd_type | No | 2 | SCD Type: 1 or 2 (default: 2) |
| key_columns | Conditional | sale_id,sale_date | Comma-separated key columns (required for SCD Type 2) |
| serverless | No | true | Use serverless compute: true or false |
| schedule_type | Yes | cron | Schedule type: cron or continuous |
| cron_expression | Conditional | 0 0 2 * * ? | Cron expression (required if schedule_type=cron) |
| timezone | Conditional | America/New_York | Timezone (required if schedule_type=cron) |
| notification | No | analytics-team@jll.com,sales-ops@jll.com | Comma-separated email addresses |
| tags | No | environment=production,dataset=daily_sales | Comma-separated key=value pairs |
| ext_catalog_name | No | finance_catalog | OVERRIDE: Custom target catalog (overrides share-level) |
| ext_schema_name | No | revenue_reports | OVERRIDE: Custom target schema (overrides share-level) |

**Multiple rows:** One row per pipeline (typically one pipeline per share_asset)

**IMPORTANT:** The `source_asset` column explicitly specifies which share_asset this pipeline processes.

**Example rows:**
```
share_name,name_prefix,source_asset,scd_type,key_columns,serverless,schedule_type,cron_expression,timezone,notification,tags,ext_catalog_name,ext_schema_name
sales_analytics_share,sales_daily_sync,main_catalog.sales_schema.daily_sales,2,sale_id|sale_date,false,cron,0 0 2 * * ?,America/New_York,analytics-team@jll.com,environment=production,dataset=daily_sales,,
sales_analytics_share,sales_orders_sync,main_catalog.sales_schema.customer_orders,2,order_id|customer_id,true,cron,0 0 */6 * * ?,UTC,analytics-team@jll.com,environment=production,,,
operations_realtime_share,ops_health_stream,operations_catalog.metrics_schema.system_health,1,,true,continuous,,,ops-team@jll.com,streaming=true,,
```

---

## Key Changes in v2.0

### 1. **Explicit source_asset Field**
- **OLD (v1):** Asset name was extracted from schedule keys
- **NEW (v2):** Each pipeline has explicit `source_asset` column specifying which share_asset it processes

**Example:**
```
# OLD v1 (DON'T USE):
Pipeline schedule key: daily_sales
System had to guess: "catalog.schema.daily_sales"

# NEW v2 (CORRECT):
source_asset: main_catalog.sales_schema.daily_sales
```

### 2. **Pipeline-Level Catalog/Schema Overrides**
- Pipelines can now override the default target catalog/schema
- Use `ext_catalog_name` and `ext_schema_name` columns in pipelines sheet
- Priority: pipeline-level > share-level

**Example:**
```
Share default: analytics_prod.shared_sales
Pipeline override: finance_catalog.revenue_reports (takes precedence)
```

### 3. **Timezone Support**
- Required for all cron-based schedules
- Use standard timezone strings: America/New_York, Europe/London, Asia/Tokyo, UTC
- Continuous schedules don't need timezone

### 4. **Simplified Schedule Structure**
- `schedule_type`: cron or continuous
- `cron_expression`: Cron string (if type=cron)
- `timezone`: Timezone string (if type=cron)

---

## Complete Example Excel Data

### metadata sheet:
```csv
requestor,project_name,business_line,strategy,description,delta_share_region,configurator,approver,executive_team,approver_status,workspace_url,version,contact_email
test.user@jll.com,SharePack Demo - Q1 2024,Data Platform Engineering,NEW,Demo SharePack,AM,data-platform-team@jll.com,analytics-leadership@jll.com,data-governance-team@jll.com,approved,https://adb-1234567890123456.12.azuredatabricks.net,2.0,test.user@jll.com
```

### recipient sheet:
```csv
name,type,recipient,recipient_databricks_org,recipient_ips,token_expiry,token_rotation,comment
external_partner_recipient,D2O,partner@external-company.com,,203.0.113.0/24,90,false,External partner for Q1 sales analytics
internal_analytics_team,D2D,analytics-team@jll.com,aws:us-west-2:a1b2c3d4-e5f6-7890-abcd-ef1234567890,,,false,Internal analytics team workspace
```

### share sheet:
```csv
share_name,comment,recipients,share_assets,ext_catalog_name,ext_schema_name,prefix_assetname,tags
sales_analytics_share,Q1 sales analytics data,external_partner_recipient|internal_analytics_team,main_catalog.sales_schema.daily_sales|main_catalog.sales_schema.customer_orders|main_catalog.sales_schema.revenue_summary,analytics_prod,shared_sales,prod_,production|sales_analytics
operations_realtime_share,Real-time operational metrics,internal_analytics_team,operations_catalog.metrics_schema.system_health|operations_catalog.metrics_schema.error_logs,ops_analytics,realtime_metrics,rt_,production|operations|monitoring
```

### pipelines sheet:
```csv
share_name,name_prefix,source_asset,scd_type,key_columns,serverless,schedule_type,cron_expression,timezone,notification,tags,ext_catalog_name,ext_schema_name
sales_analytics_share,sales_daily_sync,main_catalog.sales_schema.daily_sales,2,sale_id|sale_date,false,cron,0 0 2 * * ?,America/New_York,analytics-team@jll.com|sales-ops@jll.com,environment=production|dataset=daily_sales|owner=sales_team,,
sales_analytics_share,sales_orders_sync,main_catalog.sales_schema.customer_orders,2,order_id|customer_id,true,cron,0 0 */6 * * ?,UTC,analytics-team@jll.com,environment=production|dataset=customer_orders|owner=sales_team,,
sales_analytics_share,revenue_summary_sync,main_catalog.sales_schema.revenue_summary,1,,true,cron,0 0 * * * ?,America/New_York,finance-team@jll.com|analytics-team@jll.com,environment=production|dataset=revenue|owner=finance_team,finance_catalog,revenue_reports
operations_realtime_share,ops_health_stream,operations_catalog.metrics_schema.system_health,1,,true,continuous,,,ops-team@jll.com,environment=production|table=system_health|streaming=true,,
operations_realtime_share,ops_errors_sync,operations_catalog.metrics_schema.error_logs,1,,true,cron,0 */15 * * * ?,UTC,ops-team@jll.com|sre-team@jll.com,environment=production|table=error_logs,,
```

---

## Validation Rules

1. **One pipeline per share_asset**: Each asset in `share_assets` must have a corresponding pipeline
2. **source_asset must exist**: Pipeline's `source_asset` must match an asset in the share's `share_assets` list
3. **3-part asset names**: All assets must be fully qualified: `catalog.schema.table`
4. **Recipient references**: All recipient names in `recipients` column must exist in recipient sheet
5. **Cron + timezone**: If `schedule_type=cron`, both `cron_expression` and `timezone` are required
6. **D2D metastore**: If `type=D2D`, `recipient_databricks_org` is required
7. **Unique names**: All `name`, `share_name`, and `name_prefix` values must be unique

---

## Cell Separators

For multi-value cells (recipients, share_assets, notification, tags, key_columns):
- **Recommended:** Use pipe `|` separator
- **Alternative:** Use comma `,` separator (but avoid if values contain commas)

**Examples:**
```
recipients: external_partner_recipient|internal_analytics_team
share_assets: catalog.schema.table1|catalog.schema.table2
key_columns: id|timestamp|user_id
tags: environment=production|dataset=sales|owner=team_a
```

---

## Common Timezone Values

| Region | Timezone String |
|--------|----------------|
| US Eastern | America/New_York |
| US Central | America/Chicago |
| US Mountain | America/Denver |
| US Pacific | America/Los_Angeles |
| UK | Europe/London |
| Europe Central | Europe/Paris |
| India | Asia/Kolkata |
| Japan | Asia/Tokyo |
| Australia | Australia/Sydney |
| UTC | UTC |

---

## Testing Your Excel File

After creating your Excel file:

1. Save as `.xlsx` format (Excel 2007+)
2. Upload via API endpoint: `POST /workflow/sharepack/upload_and_validate`
3. Include header: `X-Workspace-URL: https://your-workspace.azuredatabricks.net`
4. Monitor status: `GET /workflow/sharepack/{share_pack_id}`

The system will validate structure, detect optimal strategy, and queue for provisioning.
