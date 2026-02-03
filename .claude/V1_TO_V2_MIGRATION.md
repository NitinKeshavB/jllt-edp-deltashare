# SharePack v1.0 to v2.0 Migration Guide

## Backwards Compatibility

**Good news!** The system now supports both v1.0 and v2.0 formats automatically. Your existing SharePacks will continue to work without modification.

## How It Works

When you upload a v1.0 SharePack, the system:
1. Detects the old schedule format
2. Extracts the `source_asset` from the schedule key
3. Migrates the schedule structure to v2.0 format
4. Logs a warning message suggesting upgrade to v2.0
5. Provisions normally

## Format Comparison

### v1.0 Format (Still Supported)

```yaml
pipelines:
  - name_prefix: my_pipeline
    scd_type: "2"
    key_columns: "id,timestamp"
    schedule:
      catalog.schema.table:  # Asset name as key
        cron: "0 0 2 * * ?"
        timezone: "UTC"
```

### v2.0 Format (Recommended)

```yaml
pipelines:
  - name_prefix: my_pipeline
    source_asset: catalog.schema.table  # Explicit field
    scd_type: "2"
    key_columns: "id,timestamp"
    schedule:
      cron: "0 0 2 * * ?"
      timezone: "UTC"
```

## Migration Steps (Optional but Recommended)

### For YAML Files

**Find and Replace Pattern:**

1. **Identify old pipelines:**
   ```yaml
   # OLD:
   pipelines:
     - name_prefix: sales_pipeline
       schedule:
         catalog.sales_schema.daily_sales:  # ← Asset name here
           cron: "0 0 2 * * ?"
           timezone: "UTC"
   ```

2. **Extract asset name and restructure:**
   ```yaml
   # NEW:
   pipelines:
     - name_prefix: sales_pipeline
       source_asset: catalog.sales_schema.daily_sales  # ← Moved here
       schedule:
         cron: "0 0 2 * * ?"
         timezone: "UTC"
   ```

3. **For continuous schedules:**
   ```yaml
   # OLD:
   schedule:
     catalog.schema.table: "continuous"

   # NEW:
   source_asset: catalog.schema.table
   schedule: "continuous"
   ```

### For Excel Files

**Update pipelines sheet:**

1. **Add new column** `source_asset` (after `name_prefix`)

2. **Extract asset names** from schedule structure:
   - If schedule was `{catalog.schema.table: {cron: "...", timezone: "..."}}`
   - Put `catalog.schema.table` in the `source_asset` column

3. **Flatten schedule columns:**
   - `schedule_type`: `cron` or `continuous`
   - `cron_expression`: The cron string (if type=cron)
   - `timezone`: The timezone (if type=cron)

4. **Remove old schedule dict column** (if you had one)

## Automated Migration Script (Python)

```python
#!/usr/bin/env python3
"""Migrate v1.0 SharePack YAML to v2.0 format."""

import yaml
import sys
from pathlib import Path

def migrate_pipeline(pipeline: dict) -> dict:
    """Migrate a single pipeline from v1.0 to v2.0."""
    schedule = pipeline.get("schedule")

    # Check if this is v1.0 format
    if isinstance(schedule, dict):
        schedule_keys = list(schedule.keys())

        # v1.0 format: single key that's not "cron" or "timezone"
        if len(schedule_keys) == 1 and schedule_keys[0] not in ["cron", "timezone"]:
            asset_name = schedule_keys[0]
            schedule_value = schedule[asset_name]

            # Add source_asset
            pipeline["source_asset"] = asset_name

            # Flatten schedule
            if isinstance(schedule_value, str):
                # Continuous
                pipeline["schedule"] = schedule_value
            elif isinstance(schedule_value, dict):
                # Cron with timezone
                pipeline["schedule"] = schedule_value

            print(f"  ✓ Migrated pipeline '{pipeline['name_prefix']}': source_asset={asset_name}")

    return pipeline

def migrate_sharepack(input_file: Path, output_file: Path = None):
    """Migrate entire SharePack YAML from v1.0 to v2.0."""

    if output_file is None:
        output_file = input_file.with_stem(f"{input_file.stem}_v2")

    print(f"📄 Reading: {input_file}")
    with open(input_file, 'r') as f:
        data = yaml.safe_load(f)

    print(f"🔄 Migrating pipelines...")

    migrated_count = 0
    for share in data.get("share", []):
        for pipeline in share.get("pipelines", []):
            original = pipeline.copy()
            migrated = migrate_pipeline(pipeline)
            if migrated.get("source_asset") and not original.get("source_asset"):
                migrated_count += 1

    print(f"\n✅ Migrated {migrated_count} pipelines to v2.0 format")

    print(f"💾 Writing: {output_file}")
    with open(output_file, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"\n🎉 Done! New file: {output_file}")
    print(f"\nNext steps:")
    print(f"1. Review {output_file}")
    print(f"2. Test upload to API")
    print(f"3. Replace old file if successful")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python migrate_v1_to_v2.py <input_yaml> [output_yaml]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    migrate_sharepack(input_path, output_path)
```

**Usage:**
```bash
# Automatic output file (input_v2.yaml)
python migrate_v1_to_v2.py my_sharepack.yaml

# Custom output file
python migrate_v1_to_v2.py my_sharepack.yaml my_sharepack_migrated.yaml
```

## Benefits of Migrating to v2.0

1. **Clearer structure**: Explicit `source_asset` field is more readable
2. **Better validation**: Easier to validate asset references
3. **Future-proof**: New features will target v2.0 format
4. **Easier debugging**: Less ambiguous when troubleshooting
5. **Better tooling**: IDE autocomplete and schema validation work better

## Detection of Old Format

When the system detects v1.0 format, you'll see log messages like:

```
⚠️  [MIGRATION] Pipeline 'sales_pipeline': Migrated v1.0 schedule format.
   Extracted source_asset='catalog.schema.table' from schedule.
   Please update to v2.0 format (explicit source_asset field).
```

This is informational - provisioning will continue normally.

## Validation Errors

If you see errors like:

```
Pipeline 'my_pipeline': source_asset is required.
Use v2.0 format with explicit source_asset field.
```

This means:
- The schedule has v2.0 format (cron + timezone as top-level keys)
- But `source_asset` field is missing

**Fix:** Add explicit `source_asset` field to the pipeline.

## FAQ

### Q: Do I need to migrate immediately?
**A:** No. v1.0 format continues to work. Migrate when convenient.

### Q: Can I mix v1.0 and v2.0 pipelines?
**A:** Yes. Each pipeline is validated independently. You can have some pipelines in v1.0 and others in v2.0 within the same SharePack.

### Q: What if my v1.0 SharePack fails?
**A:** The migration is applied during parsing. If it fails, you'll see a clear error message. Check:
1. Schedule has exactly one key (the asset name)
2. Schedule value is either "continuous" or a dict with cron/timezone

### Q: Will this affect my existing SharePacks in the database?
**A:** No. The migration happens during upload/parsing. Existing SharePacks in the database are unchanged.

### Q: Is the migration reversible?
**A:** The system doesn't modify your original file. You can always keep using v1.0 format if needed.

## Examples

### Example 1: Daily Batch Pipeline

**v1.0:**
```yaml
- name_prefix: daily_sync
  scd_type: "2"
  key_columns: "id,date"
  schedule:
    production.sales.daily_transactions:
      cron: "0 0 2 * * ?"
      timezone: "America/New_York"
```

**v2.0:**
```yaml
- name_prefix: daily_sync
  source_asset: production.sales.daily_transactions
  scd_type: "2"
  key_columns: "id,date"
  schedule:
    cron: "0 0 2 * * ?"
    timezone: "America/New_York"
```

### Example 2: Continuous Streaming

**v1.0:**
```yaml
- name_prefix: realtime_stream
  scd_type: "1"
  schedule:
    production.events.clickstream: "continuous"
```

**v2.0:**
```yaml
- name_prefix: realtime_stream
  source_asset: production.events.clickstream
  scd_type: "1"
  schedule: "continuous"
```

### Example 3: Multiple Pipelines

**v1.0:**
```yaml
pipelines:
  - name_prefix: pipeline_1
    schedule:
      catalog.schema.table1:
        cron: "0 0 * * * ?"
        timezone: "UTC"
  - name_prefix: pipeline_2
    schedule:
      catalog.schema.table2:
        cron: "0 0 */6 * * ?"
        timezone: "UTC"
```

**v2.0:**
```yaml
pipelines:
  - name_prefix: pipeline_1
    source_asset: catalog.schema.table1
    schedule:
      cron: "0 0 * * * ?"
      timezone: "UTC"
  - name_prefix: pipeline_2
    source_asset: catalog.schema.table2
    schedule:
      cron: "0 0 */6 * * ?"
      timezone: "UTC"
```

## Support

If you encounter issues with the automatic migration:
1. Check the error message carefully - it indicates what's wrong
2. Manually add `source_asset` field to the problematic pipeline
3. Use the migration script above to convert entire files
4. Refer to `sample_sharepack_v2.yaml` for complete v2.0 examples

---

**Version:** 2.0
**Backwards Compatible:** Yes
**Migration Required:** No (but recommended)
