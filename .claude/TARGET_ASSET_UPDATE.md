# Target Asset Configuration Update

## Changes Made

### 1. Removed `prefix_assetname` from DeltaShareConfig

**Before:**
```yaml
delta_share:
  ext_catalog_name: analytics_prod
  ext_schema_name: shared_sales
  prefix_assetname: "prod_"  # ❌ REMOVED
  tags:
    - production
```

**After:**
```yaml
delta_share:
  ext_catalog_name: analytics_prod
  ext_schema_name: shared_sales
  tags:
    - production
```

### 2. Added `target_asset` to PipelineConfig

**Before:**
```yaml
pipelines:
  - name_prefix: sales_daily_sync
    source_asset: main_catalog.sales_schema.daily_sales
    scd_type: "2"
    # Target table name was derived from prefix_assetname + source table
```

**After:**
```yaml
pipelines:
  - name_prefix: sales_daily_sync
    source_asset: main_catalog.sales_schema.daily_sales
    target_asset: prod_daily_sales  # ✅ NEW: Explicit target table name
    scd_type: "2"
```

### 3. Updated Pipeline Configuration

Pipeline configuration now uses `pipelines.target_table` instead of `pipelines.target_table_prefix`:

**Before:**
```python
configuration = {
    "pipelines.source_table": source_asset,
    "pipelines.target_table_prefix": delta_share_config.get("prefix_assetname", ""),
    # ...
}
```

**After:**
```python
configuration = {
    "pipelines.source_table": source_asset,
    "pipelines.target_table": target_asset,  # Direct table name
    # ...
}
```

## Benefits

### 1. **Explicit Target Control**
Each pipeline explicitly specifies its target table name, no more prefix concatenation:
- **Old:** `prefix_assetname + source_table_name` (implicit)
- **New:** `target_asset` (explicit)

### 2. **Flexible Naming**
Target tables can have any name, not constrained by source table names:
```yaml
source_asset: catalog.sales_schema.daily_sales_raw
target_asset: processed_sales_data  # Completely different name
```

### 3. **Clear Intent**
Reading the YAML, you immediately know the target table name without mental calculation:
```yaml
- name_prefix: sales_sync
  source_asset: raw.sales.data
  target_asset: prod_sales  # Clear!
```

## File Changes

| File | Change | Line(s) |
|------|--------|---------|
| **share_pack.py** | Removed `prefix_assetname` from DeltaShareConfig | 150 |
| **share_pack.py** | Added `target_asset: Optional[str]` to PipelineConfig | 183 |
| **provisioning.py** | Extract `target_asset` from pipeline config | 262-268 |
| **provisioning.py** | Use `pipelines.target_table` instead of `pipelines.target_table_prefix` | 269-274 |
| **sample_sharepack_v2.yaml** | Removed all `prefix_assetname` fields | 61, 142, 212 |
| **sample_sharepack_v2.yaml** | Added `target_asset` to all pipelines | Multiple |

## Code Changes Details

### Pydantic Model: DeltaShareConfig

```python
class DeltaShareConfig(BaseModel):
    """Target workspace configuration for pipelines."""

    ext_catalog_name: str  # Target catalog name
    ext_schema_name: str  # Target schema name
    # prefix_assetname: str = ""  ❌ REMOVED
    tags: List[str] = Field(default_factory=list)
```

### Pydantic Model: PipelineConfig

```python
class PipelineConfig(BaseModel):
    """Pipeline configuration for a share."""

    name_prefix: str
    source_asset: Optional[str] = None  # Source table
    target_asset: Optional[str] = None  # ✅ NEW: Target table name
    schedule: Union[CronSchedule, str, Dict[str, Any]]
    # ... other fields
```

### Provisioning Logic

```python
# Get target_asset from pipeline config
target_asset = pipeline_config.get("target_asset")

if not target_asset:
    # Default: use source asset table name if not specified
    target_asset = source_asset.split(".")[-1] if source_asset else ""

logger.info(f"Creating pipeline: {pipeline_name} (source: {source_asset}, target: {target_asset})")

# Build configuration dictionary
configuration = {
    "pipelines.source_table": source_asset,
    "pipelines.target_table": target_asset,  # Direct table name
    "pipelines.keys": pipeline_config.get("key_columns", ""),
    "pipelines.scd_type": pipeline_config.get("scd_type", "2"),
}
```

## Default Behavior

If `target_asset` is not specified, the system defaults to using the source table name:

```python
# Example: source_asset = "catalog.schema.my_table"
# Default target_asset = "my_table" (last part)
target_asset = source_asset.split(".")[-1]
```

**Example:**
```yaml
# Without target_asset
source_asset: catalog.sales.daily_transactions
# → target_asset defaults to: daily_transactions

# With explicit target_asset
source_asset: catalog.sales.daily_transactions
target_asset: processed_transactions  # Override
```

## Target Table Full Path

The full target table path is constructed as:
```
{ext_catalog_name}.{ext_schema_name}.{target_asset}
```

**Example:**
```yaml
delta_share:
  ext_catalog_name: analytics_prod
  ext_schema_name: shared_sales

pipelines:
  - source_asset: raw.sales.data
    target_asset: prod_sales_data
    ext_catalog_name: finance_catalog  # Override
    ext_schema_name: revenue_reports   # Override
```

**Results in:**
- Default target: `analytics_prod.shared_sales.prod_sales_data`
- This pipeline: `finance_catalog.revenue_reports.prod_sales_data`

## Migration from Old Format

### Old SharePacks (with prefix_assetname)

**Will still work** because:
1. `prefix_assetname` is ignored (not used in provisioning logic)
2. If `target_asset` is missing, defaults to source table name
3. No breaking changes - existing YAMLs continue to work

### Recommended Migration

Update your SharePacks to:
1. Remove `prefix_assetname` from `delta_share`
2. Add explicit `target_asset` to each pipeline

**Before:**
```yaml
delta_share:
  ext_catalog_name: analytics
  ext_schema_name: shared
  prefix_assetname: "prod_"

pipelines:
  - name_prefix: sync_sales
    source_asset: raw.sales.transactions
```

**After:**
```yaml
delta_share:
  ext_catalog_name: analytics
  ext_schema_name: shared

pipelines:
  - name_prefix: sync_sales
    source_asset: raw.sales.transactions
    target_asset: prod_transactions  # Explicit
```

## Examples

### Example 1: Basic Pipeline

```yaml
pipelines:
  - name_prefix: daily_sales_sync
    source_asset: main_catalog.sales_schema.daily_sales
    target_asset: prod_daily_sales  # Target in analytics_prod.shared_sales
    scd_type: "2"
    key_columns: "sale_id,sale_date"
    schedule:
      cron: "0 0 2 * * ?"
      timezone: "America/New_York"
```

**Creates:**
- Pipeline: `daily_sales_sync`
- Source: `main_catalog.sales_schema.daily_sales`
- Target: `analytics_prod.shared_sales.prod_daily_sales`

### Example 2: Custom Target Location

```yaml
pipelines:
  - name_prefix: revenue_summary
    source_asset: sales_catalog.analytics.revenue_data
    target_asset: revenue_summary_daily
    ext_catalog_name: finance_catalog  # Override
    ext_schema_name: revenue_reports   # Override
    scd_type: "1"
    schedule:
      cron: "0 0 * * * ?"
      timezone: "UTC"
```

**Creates:**
- Pipeline: `revenue_summary`
- Source: `sales_catalog.analytics.revenue_data`
- Target: `finance_catalog.revenue_reports.revenue_summary_daily`

### Example 3: Default Target (no target_asset specified)

```yaml
pipelines:
  - name_prefix: error_logs_sync
    source_asset: ops_catalog.metrics.error_logs
    # target_asset not specified - defaults to "error_logs"
    scd_type: "1"
    schedule:
      cron: "0 */15 * * * ?"
      timezone: "UTC"
```

**Creates:**
- Pipeline: `error_logs_sync`
- Source: `ops_catalog.metrics.error_logs`
- Target: `ops_analytics.realtime_metrics.error_logs` (defaults to source table name)

## Validation

SharePack validation now checks:
- ✅ `source_asset` is specified (or extracted from v1.0 format)
- ✅ `target_asset` is optional (defaults to source table name if missing)
- ✅ `ext_catalog_name` and `ext_schema_name` are specified (required)

## Testing

Updated sample file: [sample_sharepack_v2.yaml](sample_sharepack_v2.yaml)

All pipelines now include explicit `target_asset` fields demonstrating best practices.

---

**Status:** ✅ Implemented
**Breaking Changes:** None (backwards compatible)
**Migration Required:** Optional (recommended for clarity)
