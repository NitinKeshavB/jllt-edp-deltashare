# Pipeline Schedule Management Guide

## Overview

The workflow now supports comprehensive schedule management for pipelines in both NEW and UPDATE strategies:

- **NEW Strategy**: Automatically creates schedules for new pipelines
- **UPDATE Strategy**: Supports add, update, and remove operations for existing pipeline schedules

## YAML Configuration Examples

### 1. Add a New Schedule (UPDATE Strategy)

If pipeline has no schedule, this will create one:

```yaml
metadata:
  strategy: UPDATE

shares:
  - name: my_share
    pipelines:
      - name_prefix: wd_pipeline_1
        source_asset: catalog.schema.source_table
        schedule:
          cron: "0 0 0 * * ?"  # Daily at midnight
          timezone: "UTC"
```

**Result**: Creates new schedule for the pipeline

---

### 2. Update an Existing Schedule (UPDATE Strategy)

If pipeline already has a schedule, this will update the cron expression and/or timezone:

```yaml
metadata:
  strategy: UPDATE

shares:
  - name: my_share
    pipelines:
      - name_prefix: wd_pipeline_1
        source_asset: catalog.schema.source_table
        schedule:
          cron: "0 0 6 * * ?"  # Daily at 6 AM (changed from midnight)
          timezone: "America/New_York"  # Changed from UTC
```

**Result**:
- Updates cron expression if changed
- Updates timezone if changed
- Logs "unchanged" if both are the same

---

### 3. Remove a Schedule (UPDATE Strategy)

To delete all schedules for a pipeline:

```yaml
metadata:
  strategy: UPDATE

shares:
  - name: my_share
    pipelines:
      - name_prefix: wd_pipeline_1
        source_asset: catalog.schema.source_table
        schedule:
          action: "remove"
```

**Result**: Deletes all schedules associated with the pipeline

---

### 4. No Schedule Changes (UPDATE Strategy)

If you don't want to modify the schedule, simply omit the `schedule` field:

```yaml
metadata:
  strategy: UPDATE

shares:
  - name: my_share
    pipelines:
      - name_prefix: wd_pipeline_1
        source_asset: catalog.schema.source_table
        # No schedule field = no changes to existing schedule
        key_columns: "id,timestamp"  # Only update key columns
```

**Result**: Pipeline updated, schedule left unchanged

---

## How It Works

### UPDATE Strategy Logic

1. **Check schedule config**
   - If missing/null → skip schedule management (no changes)
   - If present → proceed to determine operation

2. **Determine operation**
   - If `action: "remove"` → **Remove** all schedules for pipeline
   - If has `cron` expression → Check if schedule exists:
     - Exists → **Update** cron/timezone if changed
     - Doesn't exist → **Create** new schedule

3. **Error handling**
   - All schedule operations raise exceptions on failure
   - Triggers rollback and marks share pack as FAILED
   - Non-retryable errors (validation) fail immediately
   - Retryable errors (timeouts) retry once after 10 minutes

### NEW Strategy Logic

1. **Create schedule** if specified in config
2. Skip if schedule is missing or continuous (not yet supported)

---

## Complete Example: Multi-Pipeline Schedule Management

```yaml
metadata:
  requestor: john.doe@company.com
  business_line: finance
  project_name: daily_reports
  strategy: UPDATE

shares:
  - name: finance_share
    delta_share:
      ext_catalog_name: external_catalog
      ext_schema_name: finance_schema

    pipelines:
      # Pipeline 1: Update schedule to run twice daily
      - name_prefix: finance_pipeline_daily
        source_asset: catalog.schema.transactions
        target_asset: transactions_external
        key_columns: "transaction_id,timestamp"
        schedule:
          cron: "0 0 0,12 * * ?"  # Midnight and noon
          timezone: "America/New_York"

      # Pipeline 2: Add a new schedule (previously had none)
      - name_prefix: finance_pipeline_weekly
        source_asset: catalog.schema.weekly_summary
        target_asset: weekly_summary_external
        key_columns: "week_id"
        schedule:
          cron: "0 0 9 ? * MON"  # Every Monday at 9 AM
          timezone: "America/New_York"

      # Pipeline 3: Remove schedule (will be triggered manually)
      - name_prefix: finance_pipeline_manual
        source_asset: catalog.schema.manual_reports
        target_asset: manual_reports_external
        key_columns: "report_id"
        schedule:
          action: "remove"

      # Pipeline 4: No schedule changes (only update notifications)
      - name_prefix: finance_pipeline_adhoc
        source_asset: catalog.schema.adhoc_data
        target_asset: adhoc_data_external
        notification:
          - newteam@company.com  # Add new notification email
```

---

## Cron Expression Format

Databricks uses Quartz cron format with 6 fields:

```
┌───────────── second (0-59)
│ ┌───────────── minute (0-59)
│ │ ┌───────────── hour (0-23)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12 or JAN-DEC)
│ │ │ │ │ ┌───────────── day of week (0-6 or SUN-SAT, 0=Sunday)
│ │ │ │ │ │
* * * * * *
```

### Common Examples

- `0 0 0 * * ?` - Daily at midnight
- `0 0 6 * * ?` - Daily at 6 AM
- `0 0 0,12 * * ?` - Daily at midnight and noon
- `0 0 9 ? * MON` - Every Monday at 9 AM
- `0 0 18 ? * MON-FRI` - Weekdays at 6 PM
- `0 0 0 1 * ?` - First day of every month at midnight

---

## Error Scenarios

### Scenario 1: Failed Schedule Update

```yaml
schedule:
  cron: "invalid cron"  # Invalid format
```

**Result**:
- Error: `RuntimeError: Failed to update cron for pipeline_1: Error updating schedule...`
- Share pack status: FAILED
- No retry (non-retryable error)

### Scenario 2: Timeout During Schedule Creation

```yaml
schedule:
  cron: "0 0 0 * * ?"
```

**Result** (if Databricks API times out):
- Error: `ReadTimeout`
- Status: IN_PROGRESS
- Retry: Once after 10 minutes (retryable error)
- If retry succeeds → COMPLETED
- If retry fails → FAILED with "Retried failed request and stopping"

---

## Benefits

1. **Flexibility**: Add, update, or remove schedules without recreating pipelines
2. **Safety**: All operations raise exceptions on failure and trigger rollback
3. **Clarity**: Clear logging of what changed vs. unchanged
4. **Idempotency**: Re-running same config produces same result
5. **Granular Control**: Manage schedules independently from pipeline configuration

---

## Next Steps

To use schedule management:

1. Upload share pack YAML with schedule configuration
2. Monitor share pack status via `/workflow/sharepack/{share_pack_id}` endpoint
3. Check logs for schedule operation details
4. Verify schedules in Databricks Workflows UI
