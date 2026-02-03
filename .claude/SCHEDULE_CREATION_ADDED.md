# ✅ Schedule Creation Added to Provisioning

## What Was Added

The provisioning orchestrator now creates Databricks job schedules for DLT pipelines automatically.

## How It Works

After creating or finding each pipeline, the system:

1. **Extracts Pipeline ID** - Gets the `pipeline_id` from the created pipeline or looks it up if it already exists
2. **Parses Schedule Config** - Reads the schedule configuration from the pipeline config
3. **Creates Job Schedule** - Calls `create_schedule_for_pipeline()` to create a Databricks job

### Schedule Creation Flow

```
Pipeline Created/Found
  ↓
Extract pipeline_id
  ↓
Parse schedule config
  ↓
Create Databricks Job
  ↓
Job runs pipeline on schedule
```

## Supported Schedule Types

### 1. Cron-Based Schedules

```yaml
pipelines:
  - name_prefix: sales_daily_sync
    source_asset: catalog.schema.sales_data
    schedule:
      cron: "0 0 2 * * ?"  # Daily at 2 AM
      timezone: "America/New_York"
```

**Creates:** Databricks job with cron schedule

### 2. Continuous Schedules

```yaml
pipelines:
  - name_prefix: realtime_stream
    source_asset: catalog.schema.streaming_data
    schedule: "continuous"
```

**Status:** Not yet implemented (logs warning, skips schedule creation)

## Code Changes

### File: [provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py)

#### 1. Added Imports

```python
from dbrx_api.jobs.dbrx_pipelines import create_pipeline, list_pipelines_with_search_criteria
from dbrx_api.jobs.dbrx_schedule import create_schedule_for_pipeline
```

#### 2. Added Pipeline ID Extraction

```python
# After pipeline creation
if isinstance(result, str):
    # Pipeline already exists
    pipeline_id = None  # Look up later
else:
    # Pipeline created
    pipeline_id = result.pipeline_id
```

#### 3. Added Pipeline ID Lookup (for existing pipelines)

```python
if pipeline_id is None:
    # Pipeline already existed - look up its ID
    pipelines = list_pipelines_with_search_criteria(
        dltshr_workspace_url=workspace_url,
        filter_expr=pipeline_name,
    )
    for pipeline in pipelines:
        if pipeline.name == pipeline_name:
            pipeline_id = pipeline.pipeline_id
            break
```

#### 4. Added Schedule Creation Logic

```python
if pipeline_id and schedule:
    job_name = f"{pipeline_name}_schedule"

    # Handle different schedule formats
    if isinstance(schedule, str):
        # Continuous (not yet implemented)
        logger.warning("Continuous schedules not yet implemented")

    elif isinstance(schedule, dict):
        # Cron schedule
        cron_expression = schedule.get("cron")
        timezone = schedule.get("timezone", "UTC")

        if cron_expression:
            schedule_result = create_schedule_for_pipeline(
                dltshr_workspace_url=workspace_url,
                job_name=job_name,
                pipeline_id=pipeline_id,
                cron_expression=cron_expression,
                time_zone=timezone,
                paused=False,
                email_notifications=pipeline_config.get("notification", []),
                tags=pipeline_config.get("tags", {}),
            )
```

## Schedule Naming Convention

Job schedules are named: `{pipeline_name}_schedule`

**Example:**
- Pipeline: `sales_daily_sync`
- Schedule Job: `sales_daily_sync_schedule`

## Idempotency

The schedule creation is idempotent:
- If a job with the same name already exists, it logs a warning and continues
- Provisioning doesn't fail if schedule already exists
- Schedule creation failures don't fail the entire provisioning (logged as errors)

## What You'll See in Logs

### Successful Schedule Creation

```
✓ Created pipeline: sales_daily_sync (id: abc-123-def-456)
  Creating schedule for pipeline: sales_daily_sync (job: sales_daily_sync_schedule)
✓ Created schedule: sales_daily_sync_schedule (cron: 0 0 2 * * ?, tz: America/New_York)
```

### Existing Schedule

```
⚠️  Pipeline sales_daily_sync already exists, skipping creation
   Found existing pipeline ID: abc-123-def-456
   Creating schedule for pipeline: sales_daily_sync (job: sales_daily_sync_schedule)
⚠️  Schedule sales_daily_sync_schedule already exists, skipping creation
```

### Continuous Schedule (Not Implemented)

```
✓ Created pipeline: realtime_stream (id: xyz-789-abc-012)
  Creating schedule for pipeline: realtime_stream (job: realtime_stream_schedule)
⚠️  Continuous schedule requested for realtime_stream, but continuous jobs are not yet implemented. Skipping schedule creation.
```

## Schedule Parameters

The `create_schedule_for_pipeline` function is called with:

| Parameter | Source | Description |
|-----------|--------|-------------|
| `dltshr_workspace_url` | SharePack metadata | Workspace URL |
| `job_name` | Generated | `{pipeline_name}_schedule` |
| `pipeline_id` | Pipeline creation/lookup | DLT pipeline ID |
| `cron_expression` | Pipeline config | Quartz cron (6-field) |
| `time_zone` | Pipeline config | Timezone string (default: UTC) |
| `paused` | Hardcoded | Always `False` (schedules start active) |
| `email_notifications` | Pipeline config | List of email addresses |
| `tags` | Pipeline config | Job tags |

## Error Handling

Schedule creation errors are **non-fatal**:
- Logged as errors with full stack trace
- Provisioning continues to next pipeline
- Doesn't mark SharePack as failed

This ensures that provisioning succeeds even if schedule creation fails.

## Example SharePack YAML

```yaml
share:
  - name: sales_share
    share_assets:
      - catalog.sales_schema.daily_sales
      - catalog.sales_schema.customer_orders

    delta_share:
      ext_catalog_name: analytics_prod
      ext_schema_name: shared_sales

    pipelines:
      # Pipeline 1: Daily at 2 AM EST
      - name_prefix: sales_daily_sync
        source_asset: catalog.sales_schema.daily_sales
        schedule:
          cron: "0 0 2 * * ?"
          timezone: "America/New_York"
        notification:
          - analytics-team@jll.com

      # Pipeline 2: Every 6 hours (UTC)
      - name_prefix: sales_orders_sync
        source_asset: catalog.sales_schema.customer_orders
        schedule:
          cron: "0 0 */6 * * ?"
          timezone: "UTC"
```

**Result:**
- Creates 2 pipelines
- Creates 2 Databricks jobs:
  - `sales_daily_sync_schedule` - runs daily at 2 AM EST
  - `sales_orders_sync_schedule` - runs every 6 hours (UTC)

## Databricks UI

After provisioning, you can view the schedules in Databricks:

1. **Workflows** → **Jobs**
2. Search for: `{pipeline_name}_schedule`
3. View schedule details, run history, etc.

## Future Enhancements

### Continuous Schedules

To implement continuous schedules:
1. Use `databricks.sdk.service.jobs.Continuous` instead of `CronSchedule`
2. Update `create_schedule_for_pipeline` to support continuous mode
3. Or create a separate `create_continuous_job_for_pipeline` function

**Example:**
```python
from databricks.sdk.service.jobs import Continuous

w_client.jobs.create(
    name=job_name,
    tasks=[...],
    continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
    max_concurrent_runs=1,
)
```

## Testing

To test schedule creation:

1. **Upload SharePack** with pipeline schedules
2. **Check Provisioning Logs** for schedule creation messages
3. **Verify in Databricks UI**: Workflows → Jobs
4. **Check Job Details**: Schedule, status, run history

---

**Status:** ✅ Implemented for cron-based schedules
**Continuous Schedules:** ⏳ Not yet implemented (logs warning)
**Error Handling:** ✅ Non-fatal (logs error, continues provisioning)
