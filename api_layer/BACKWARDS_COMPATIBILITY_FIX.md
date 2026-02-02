# Backwards Compatibility Fix - v1.0 SharePacks Now Supported

## Issue

Your existing SharePack YAML was using v1.0 format:
```yaml
pipelines:
  - name_prefix: wd_pipeline_1
    schedule:
      catalog.schema.table:  # Old format: asset as key
        cron: "..."
```

But the new code expected v2.0 format:
```yaml
pipelines:
  - name_prefix: wd_pipeline_1
    source_asset: catalog.schema.table  # New format: explicit field
    schedule:
      cron: "..."
```

This caused error: `KeyError: 'source_asset'`

## Fix Applied

✅ **Automatic Migration**: The system now detects v1.0 format and automatically migrates it to v2.0

### Changes Made

1. **[share_pack.py](src/dbrx_api/workflow/models/share_pack.py#L177-L233)** - Updated `PipelineConfig` model:
   - Made `source_asset` optional (backwards compatible)
   - Added `migrate_v1_to_v2_and_validate()` validator
   - Automatically extracts `source_asset` from old schedule format during upload
   - Logs migration warning for user awareness

2. **[provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py#L231-L257)** - Added runtime migration:
   - **KEY FIX**: Detects missing `source_asset` in pipeline config from database
   - Extracts from v1.0 schedule format at runtime
   - **Handles SharePacks already stored in database with old format**
   - Logs migration warnings during provisioning

3. **[V1_TO_V2_MIGRATION.md](V1_TO_V2_MIGRATION.md)** - Created migration guide:
   - Explains backwards compatibility
   - Shows format comparison
   - Provides migration script (optional)
   - Lists benefits of v2.0

4. **Type hints updated**:
   - `schedule: Union[CronSchedule, str, Dict[str, Any]]` - Now accepts old dict format
   - `source_asset: Optional[str]` - Now optional for backwards compatibility

## How It Works

### Two-Stage Migration

**Stage 1: During Upload (Pydantic validation)**
- Validates and migrates new SharePacks as they're uploaded
- Runs in `share_pack.py` PipelineConfig model

**Stage 2: During Provisioning (Runtime migration)** ⭐ **KEY FIX**
- Migrates SharePacks already stored in database with old format
- Runs in `provisioning.py` before creating pipelines
- **This fixes the error you saw** - old SharePacks in database now work

### Detection Logic (Both Stages)

```python
# Check if source_asset is missing
source_asset = pipeline_config.get("source_asset")

if source_asset is None:
    # v1.0 format: extract from schedule dict
    schedule = pipeline_config.get("schedule", {})
    if isinstance(schedule, dict):
        schedule_keys = [k for k in schedule.keys() if k not in ["cron", "timezone"]]
        if len(schedule_keys) == 1:
            source_asset = schedule_keys[0]  # Extracted!
```

### What You'll See

When using v1.0 format, you'll see log messages like:

```
⚠️  [MIGRATION] Pipeline 'wd_pipeline_1': Migrated v1.0 schedule format.
   Extracted source_asset='catalog.schema.table' from schedule.
   Please update to v2.0 format (explicit source_asset field).
```

**This is informational only** - provisioning continues normally.

## What This Means

### ✅ You Can Now:

1. **Use existing v1.0 SharePacks** - No changes needed, they work immediately
2. **Mix v1.0 and v2.0** - Some pipelines can be old format, some new
3. **Migrate at your own pace** - No rush, backwards compatibility is permanent

### 📝 Recommended (But Optional):

Migrate to v2.0 format for:
- Better readability
- Easier validation
- Future-proof structure

Use the migration guide: [V1_TO_V2_MIGRATION.md](V1_TO_V2_MIGRATION.md)

## Testing

### Your Current SharePack Should Now Work

Re-upload your existing YAML:
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload_and_validate" \
  -H "X-Workspace-URL: https://your-workspace.azuredatabricks.net" \
  -F "file=@your_sharepack.yaml"
```

**Expected behavior:**
- ✅ Upload succeeds
- ✅ Validation passes with migration warnings
- ✅ Provisioning completes successfully
- ⚠️  Log shows migration messages (informational)

### Example v1.0 Format (Now Supported)

```yaml
metadata:
  requestor: test@jll.com
  project_name: "Test Project"
  strategy: NEW
  workspace_url: "https://adb-xxx.azuredatabricks.net"
  # ... other metadata ...

recipient:
  - name: wd_recipient_d2o
    type: D2O
    # ... other fields ...

share:
  - name: WD_share_q1
    share_assets:
      - catalog.schema.table1
      - catalog.schema.table2

    delta_share:
      ext_catalog_name: target_catalog
      ext_schema_name: target_schema

    pipelines:
      # OLD v1.0 FORMAT - NOW WORKS!
      - name_prefix: wd_pipeline_1
        scd_type: "2"
        key_columns: "id,timestamp"
        schedule:
          catalog.schema.table1:  # Asset as key
            cron: "0 0 2 * * ?"
            timezone: "UTC"

      # Can also use NEW v2.0 FORMAT
      - name_prefix: wd_pipeline_2
        source_asset: catalog.schema.table2
        scd_type: "2"
        key_columns: "id,date"
        schedule:
          cron: "0 0 */6 * * ?"
          timezone: "UTC"
```

## Files Modified

| File | Change | Purpose |
|------|--------|---------|
| `src/dbrx_api/workflow/models/share_pack.py` | Updated PipelineConfig model | Backwards compatibility |
| `V1_TO_V2_MIGRATION.md` | Created | Migration guide |
| `BACKWARDS_COMPATIBILITY_FIX.md` | Created | This document |

## No Breaking Changes

- ✅ All v1.0 SharePacks continue to work
- ✅ All v2.0 SharePacks work as before
- ✅ Can mix both formats in same file
- ✅ No database migrations needed
- ✅ No configuration changes needed

## Next Steps

1. **Re-upload your SharePack** - Should work immediately
2. **Check logs** - Look for migration warnings (informational only)
3. **Optional**: Migrate to v2.0 format when convenient

---

**Status:** ✅ Fixed
**Backwards Compatible:** Yes
**Breaking Changes:** None
**Action Required:** None (migration optional)
