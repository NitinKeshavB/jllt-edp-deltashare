# SharePack Excel Format Guide

## Overview

SharePack configuration can be provided in Excel (.xlsx) format with the following structure:
- **4 sheets**: `metadata`, `recipient`, `share`, `pipelines`
- All sheets are required (even if empty)

---

## Sheet 1: `metadata`

### Structure
Two-column format: `Field` | `Value`

### Required Fields

| Field | Value | Validation |
|-------|-------|------------|
| `requestor` | john.doe@company.com | Valid email or AD group name |
| `business_line` | Finance Analytics | Text |
| `strategy` | NEW or UPDATE | Must be "NEW" or "UPDATE" |
| `delta_share_region` | AM or EMEA | Must be "AM" or "EMEA" |
| `configurator` | data-team@company.com | Valid email or AD group |
| `approver` | finance-leadership@company.com | Valid email or AD group |
| `executive_team` | data-governance@company.com | Valid email or AD group |
| `approver_status` | approved | approved \| declined \| request_more_info \| pending |
| `workspace_url` | https://adb-123.azuredatabricks.net | Valid HTTPS URL, must be reachable |
| `servicenow` | INC0012345 | ServiceNow ticket number or link |

### Optional Fields

| Field | Value | Notes |
|-------|-------|-------|
| `project_name` | Q1 2025 Finance | Optional project name |
| `version` | 1.0 | Optional version |
| `contact_email` | john.doe@company.com | Required if different from requestor |
| `description` | Finance analytics sharing | Optional description |

### Example

```
Field                 | Value
---------------------|---------------------------------------
requestor            | john.doe@company.com
business_line        | Finance Analytics
strategy             | NEW
delta_share_region   | AM
configurator         | data-team@company.com
approver             | finance-leadership@company.com
executive_team       | data-governance@company.com
approver_status      | approved
workspace_url        | https://adb-1234567890123456.12.azuredatabricks.net
servicenow           | INC0012345
project_name         | Q1 2025 Finance Reporting
version              | 1.0
contact_email        | john.doe@company.com
description          | Finance analytics data sharing
```

---

## Sheet 2: `recipient`

### Columns

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `name` | Yes | Unique recipient identifier | external_partner_d2o |
| `type` | Yes | D2O or D2D | D2O |
| `recipient` | Yes | Contact email | partner@external-company.com |
| `recipient_databricks_org` | D2D only | Metastore ID | aws:us-west-2:uuid |
| `recipient_ips_to_add` | D2O optional | Comma-separated IPs | 203.0.113.0/24,198.51.100.50 |
| `recipient_ips_to_remove` | UPDATE only | Comma-separated IPs to remove | 198.51.100.50 |
| `token_expiry` | D2O optional | Days (default: 90) | 90 |
| `token_rotation` | D2O optional | true or false | false |
| `comment` | Optional | Description | External analytics partner |

### Example - NEW Strategy

```
name                    | type | recipient                     | recipient_databricks_org                      | recipient_ips_to_add              | token_expiry | comment
------------------------|------|-------------------------------|-----------------------------------------------|-----------------------------------|--------------|---------------------------
external_partner_d2o    | D2O  | partner@external-company.com  |                                               | 203.0.113.0/24,198.51.100.50     | 90           | External analytics partner
internal_team_d2d       | D2D  | analytics-team@company.com    | aws:us-west-2:a1b2c3d4-e5f6-7890-abcd-ef123  |                                   |              | Internal analytics workspace
```

### Example - UPDATE Strategy

```
name                    | type | recipient                     | recipient_ips_to_add  | recipient_ips_to_remove | comment
------------------------|------|-------------------------------|-----------------------|-------------------------|---------------------------
external_partner_d2o    | D2O  | partner@external-company.com  | 203.0.113.100         | 198.51.100.50          | Updated partner IPs
internal_team_d2d       | D2D  | analytics-team@company.com    |                       |                         | Internal analytics workspace
```

**Note**: In UPDATE strategy, recipients MUST already exist in Databricks.

---

## Sheet 3: `share`

### Columns

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `name` | Yes | Share name | finance_daily_reports |
| `comment` | Optional | Description | Daily financial reporting data |
| `recipients` | Yes | Comma-separated recipient names | external_partner_d2o,internal_team_d2d |
| `share_assets` | Yes | Comma-separated 3-part table names | main_catalog.finance.daily_transactions,main_catalog.finance.revenue_summary |
| `ext_catalog_name` | Yes | Target catalog | analytics_prod |
| `ext_schema_name` | Yes | Target schema | finance_shared |
| `tags` | Optional | Comma-separated tags | production,finance |

### Example - NEW Strategy

```
name                  | comment                        | recipients                               | share_assets                                                                          | ext_catalog_name | ext_schema_name | tags
----------------------|--------------------------------|------------------------------------------|---------------------------------------------------------------------------------------|------------------|-----------------|------------------
finance_daily_reports | Daily financial reporting data | external_partner_d2o,internal_team_d2d   | main_catalog.finance.daily_transactions,main_catalog.finance.revenue_summary         | analytics_prod   | finance_shared  | production,finance
operations_realtime   | Real-time operational metrics  | internal_team_d2d                        | ops_catalog.metrics.system_health                                                     | ops_analytics    | realtime        | production,ops
```

### Example - UPDATE Strategy

```
name                  | comment                                | recipients                               | share_assets
----------------------|----------------------------------------|------------------------------------------|------------------------------------------------------
finance_daily_reports | Updated financial reporting data       | external_partner_d2o,internal_team_d2d   | main_catalog.finance.daily_transactions,main_catalog.finance.revenue_summary
```

**Note**: In UPDATE strategy, shares MUST already exist in Databricks.

---

## Sheet 4: `pipelines`

### Columns

| Column | Required | Description | Example | Validation |
|--------|----------|-------------|---------|------------|
| `share_name` | Yes | Share this pipeline belongs to | finance_daily_reports | Must match share name |
| `name_prefix` | Yes | Pipeline name | finance_transactions_pipeline | Unique identifier |
| `source_asset` | Yes | Source table (3-part name) | main_catalog.finance.daily_transactions | **IMMUTABLE in UPDATE** |
| `target_asset` | Yes | Target table name (not full path) | daily_transactions_external | Can update in UPDATE |
| `scd_type` | Yes | 1 or 2 | 2 | **IMMUTABLE in UPDATE** |
| `key_columns` | Required for Type 2 | Comma-separated columns | transaction_id,transaction_date | Must exist in source table |
| `serverless` | Optional | true or false | true | Can update in UPDATE |
| `ext_catalog_name` | Optional | Override catalog | finance_catalog | Overrides share-level |
| `ext_schema_name` | Optional | Override schema | revenue_reports | Overrides share-level |
| `notification` | Optional | Comma-separated emails | finance-ops@company.com,ops@company.com | Can update in UPDATE |
| `tags` | Optional | Semicolon-separated key:value | environment:production;owner:finance_team | Can update in UPDATE |
| `schedule_action` | Optional | remove (UPDATE only) | remove | Use "remove" to delete schedule |
| `schedule_cron` | Optional | Quartz cron (6 fields) | 0 0 2 * * ? | Can update in UPDATE |
| `schedule_timezone` | Optional | IANA timezone | America/New_York | Can update in UPDATE |
| `description` | Optional | Pipeline description | Daily finance sync | Optional |

### Example - NEW Strategy

```
share_name            | name_prefix                    | source_asset                              | target_asset                  | scd_type | key_columns                          | serverless | notification                     | tags                                      | schedule_cron   | schedule_timezone
----------------------|--------------------------------|-------------------------------------------|-------------------------------|----------|--------------------------------------|------------|----------------------------------|-------------------------------------------|-----------------|-------------------
finance_daily_reports | finance_transactions_pipeline  | main_catalog.finance.daily_transactions   | daily_transactions_external   | 2        | transaction_id,transaction_date      | true       | finance-ops@company.com          | environment:production;owner:finance_team | 0 0 2 * * ?     | America/New_York
finance_daily_reports | finance_revenue_pipeline       | main_catalog.finance.revenue_summary      | revenue_summary_external      | 1        |                                      | true       | finance-leadership@company.com   | environment:production;owner:finance_team | 0 0 6 * * ?     | UTC
operations_realtime   | ops_health_stream              | ops_catalog.metrics.system_health         | system_health_stream          | 1        |                                      | true       | ops-team@company.com             | environment:production;table:health       | continuous      | UTC
```

### Example - UPDATE Strategy

**Scenario 1: Update target_asset and schedule**
```
share_name            | name_prefix                    | source_asset                              | target_asset           | scd_type | key_columns                               | serverless | schedule_cron   | schedule_timezone
----------------------|--------------------------------|-------------------------------------------|------------------------|----------|-------------------------------------------|------------|-----------------|-------------------
finance_daily_reports | finance_transactions_pipeline  | main_catalog.finance.daily_transactions   | daily_txn_v2           | 2        | transaction_id,transaction_date,customer_id | true     | 0 0 3 * * ?     | America/New_York
```
✅ Changes target_asset (mutable)
✅ Adds customer_id to key_columns (validated)
✅ Updates schedule from 2 AM to 3 AM

**Scenario 2: Add new schedule**
```
share_name            | name_prefix               | source_asset                         | target_asset                 | scd_type | schedule_cron    | schedule_timezone
----------------------|---------------------------|--------------------------------------|------------------------------|----------|------------------|-------------------
finance_daily_reports | finance_revenue_pipeline  | main_catalog.finance.revenue_summary | revenue_summary_external     | 1        | 0 0 */12 * * ?   | UTC
```
✅ Adds new schedule (every 12 hours) to pipeline that had none

**Scenario 3: Remove schedule**
```
share_name            | name_prefix          | source_asset                      | target_asset             | scd_type | schedule_action
----------------------|----------------------|-----------------------------------|--------------------------|----------|----------------
operations_realtime   | ops_health_stream    | ops_catalog.metrics.system_health | system_health_stream     | 1        | remove
```
✅ Removes all schedules for this pipeline

**Scenario 4: Invalid - Change source_asset (WILL FAIL)**
```
share_name            | name_prefix                    | source_asset                          | target_asset                  | scd_type
----------------------|--------------------------------|---------------------------------------|-------------------------------|----------
finance_daily_reports | finance_transactions_pipeline  | main_catalog.finance.NEW_TABLE        | daily_transactions_external   | 2
```
❌ ERROR: Cannot change source_asset (IMMUTABLE field)

**Scenario 5: Invalid - Change scd_type (WILL FAIL)**
```
share_name            | name_prefix                    | source_asset                              | target_asset                  | scd_type
----------------------|--------------------------------|-------------------------------------------|-------------------------------|----------
finance_daily_reports | finance_transactions_pipeline  | main_catalog.finance.daily_transactions   | daily_transactions_external   | 1
```
❌ ERROR: Cannot change scd_type from "2" to "1" (IMMUTABLE field)

**Scenario 6: Invalid - Invalid key_columns (WILL FAIL)**
```
share_name            | name_prefix                    | source_asset                              | target_asset                  | scd_type | key_columns
----------------------|--------------------------------|-------------------------------------------|-------------------------------|----------|-------------------
finance_daily_reports | finance_transactions_pipeline  | main_catalog.finance.daily_transactions   | daily_transactions_external   | 2        | invalid_column
```
❌ ERROR: Column 'invalid_column' does not exist in source table

---

## Validation Rules Summary

### Metadata Sheet
- ✅ Email validation (valid domains, supports AD groups)
- ✅ workspace_url must be HTTPS and reachable
- ✅ servicenow is required
- ✅ strategy must be "NEW" or "UPDATE"

### Recipients Sheet
- ✅ **NEW**: Can create new recipients
- ❌ **UPDATE**: Recipients MUST exist (no creation)
- ✅ D2O: Supports IP allowlist
- ✅ D2D: Requires recipient_databricks_org

### Share Sheet
- ✅ **NEW**: Can create new shares
- ❌ **UPDATE**: Shares MUST exist (no creation)
- ✅ Each share_asset MUST have a corresponding pipeline

### Pipelines Sheet
- ✅ **NEW**: Can create new pipelines
- ❌ **UPDATE**: Pipelines MUST exist (no creation)
- ❌ **UPDATE**: Cannot change source_asset (IMMUTABLE)
- ❌ **UPDATE**: Cannot change scd_type (IMMUTABLE)
- ✅ **UPDATE**: Can change target_asset, key_columns (validated), serverless, notifications, tags, schedule
- ✅ key_columns validated against source table schema
- ✅ Schedule operations: add, update, remove

---

## Tags Format

### In YAML
```yaml
tags:
  environment: production
  owner: finance_team
  priority: high
```

### In Excel
Use semicolon-separated `key:value` pairs:
```
environment:production;owner:finance_team;priority:high
```

---

## Schedule Cron Format

Databricks uses Quartz cron with 6 fields:
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

### Common Examples
- `0 0 0 * * ?` - Daily at midnight
- `0 0 2 * * ?` - Daily at 2 AM
- `0 0 */6 * * ?` - Every 6 hours
- `0 */15 * * * ?` - Every 15 minutes
- `0 0 9 ? * MON` - Every Monday at 9 AM
- `continuous` - Real-time streaming (not yet supported, will log warning)

---

## Error Handling

### Non-Retryable Errors (Fail Immediately)
- Validation errors (invalid emails, missing fields)
- Immutable field changes (source_asset, scd_type)
- Invalid key_columns
- Resource doesn't exist (UPDATE strategy)
- Permission errors

**Result**: Status = FAILED, no retry

### Retryable Errors (Retry Once After 10 Minutes)
- Timeout errors
- Connection errors
- HTTP 503/504 errors
- Database connection errors

**Result**: Retry → If fails again → Status = FAILED with "Retried failed request and stopping"

---

## Complete Excel Example Files

Two reference files have been created:
1. `sample_sharepack_NEW_strategy.xlsx` - NEW strategy with all features
2. `sample_sharepack_UPDATE_strategy.xlsx` - UPDATE strategy with schedule management

Download these files as templates for your own share packs.

---

## Tips

1. **Start with YAML**: It's easier to read and debug
2. **Convert to Excel**: For bulk operations or team collaboration
3. **Validate locally**: Check emails, workspace URLs before uploading
4. **Test NEW first**: Create resources with NEW, then use UPDATE for modifications
5. **Check logs**: View detailed logs for validation errors
6. **Use deduplication**: Same requestor + business_line + project_name → reuses share_pack_id

---

## Next Steps

1. Choose format: YAML or Excel
2. Fill in metadata with valid governance fields
3. Define recipients (D2O or D2D)
4. Map shares and assets
5. Create pipelines (one per share_asset)
6. Add schedules
7. Upload via `/workflow/sharepack/upload_and_validate` endpoint
8. Monitor status via `/workflow/sharepack/{share_pack_id}` endpoint
