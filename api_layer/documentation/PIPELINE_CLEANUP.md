# Intelligent Pipeline Cleanup Feature

## Overview

Automatically cleans up orphaned pipelines when their source assets are removed from shares. The cleanup intelligently decides whether to delete pipelines from Databricks based on whether the asset exists in other shares.

## How It Works

### Cleanup Logic

When assets are removed from shares using `share_assets_to_remove`, the system:

1. **Identifies Orphaned Pipelines**: Finds pipelines whose `source_asset` has been removed from the share
2. **Checks Cross-Share Usage**: Determines if the removed asset exists in ANY other share
3. **Smart Deletion**:
   - **Asset NOT in other shares** → Delete pipeline + schedule from Databricks + soft-delete DB record
   - **Asset IN other shares** → Keep pipeline in Databricks (other shares need it), only soft-delete DB record

### Execution Flow

```
1. ensure_recipients      (Databricks operations)
2. ensure_shares         (Databricks operations, may remove assets)
3. ensure_pipelines      (Databricks operations, create/update from config)
4. persist_to_db         (Database writes)
5. cleanup_orphaned_pipelines  ← NEW STEP
6. mark_complete
```

## Example Scenarios

### Scenario 1: Asset Removed from All Shares

**Initial State:**
- Share A has asset `catalog.schema.table1` → Pipeline A processes it
- No other share has `catalog.schema.table1`

**Action:**
```yaml
shares:
  - name: share_a
    share_assets_to_remove:
      - catalog.schema.table1
```

**Result:**
- ✅ Pipeline A **deleted from Databricks** (schedule + pipeline)
- ✅ Pipeline A **soft-deleted in database**
- **Reason**: Asset not used by any other share

---

### Scenario 2: Asset Still in Other Shares

**Initial State:**
- Share A has asset `catalog.schema.table1` → Pipeline A processes it
- Share B has asset `catalog.schema.table1` → Pipeline B processes it

**Action:**
```yaml
shares:
  - name: share_a
    share_assets_to_remove:
      - catalog.schema.table1
```

**Result:**
- ✅ Pipeline A **kept in Databricks** (no deletion)
- ✅ Pipeline A **soft-deleted in database** (for Share A only)
- **Reason**: Asset still used by Share B

---

### Scenario 3: Multiple Pipelines, Partial Removal

**Initial State:**
- Share A has assets: `table1`, `table2`, `table3`
  - Pipeline A1 processes `table1`
  - Pipeline A2 processes `table2`
  - Pipeline A3 processes `table3`
- Share B has asset: `table2` → Pipeline B2 processes it

**Action:**
```yaml
shares:
  - name: share_a
    share_assets_to_remove:
      - table1  # Only in Share A
      - table2  # Also in Share B
```

**Result:**
- ✅ Pipeline A1 → **Deleted from Databricks** (table1 not in other shares)
- ✅ Pipeline A2 → **Kept in Databricks** (table2 still in Share B)
- ✅ Both soft-deleted in database for Share A
- ✅ Pipeline A3 → **Untouched** (table3 still in Share A)

## Implementation Details

### New Module: `pipeline_cleanup.py`

**Location**: `src/dbrx_api/workflow/orchestrator/pipeline_cleanup.py`

**Key Functions**:

1. **`cleanup_orphaned_pipelines()`**
   - Main cleanup orchestrator
   - Queries database for pipelines and shares
   - Determines which pipelines are orphaned
   - Deletes from Databricks or soft-deletes in DB

2. **`get_assets_being_removed()`**
   - Helper to determine which assets are being removed
   - Supports both declarative and explicit approaches

### Database Changes

**New Method**: `ShareRepository.list_all()`
- Returns all current shares across all share packs
- Used to check if an asset exists in other shares

```python
async def list_all(self, include_deleted: bool = False) -> List[Dict[str, Any]]:
    """Get all current shares across all share packs."""
```

### Provisioning Flow Integration

**Added to**: `provision_sharepack_new()` in `provisioning.py`

**New Step 7/9**: "Cleaning up orphaned pipelines"
- Runs after database persistence (Step 6)
- Before completion (Step 9)
- Non-fatal: Logs warnings but continues if cleanup fails

## Logging Output

### Successful Cleanup (Asset Not in Other Shares)

```
[INFO] Found 2 orphaned pipeline(s) to clean up
[INFO] Pipeline 'sales_pipeline_1': asset 'catalog.sales.old_table' not in other shares, deleting from Databricks
[INFO] Deleted schedule for pipeline 'sales_pipeline_1'
[SUCCESS] Deleted pipeline 'sales_pipeline_1' from Databricks
[INFO] Soft-deleted pipeline 'sales_pipeline_1' from database
```

### Successful Cleanup (Asset in Other Shares)

```
[INFO] Found 1 orphaned pipeline(s) to clean up
[INFO] Pipeline 'analytics_pipeline': asset 'catalog.analytics.shared_table' exists in other shares (share_b, share_c), keeping in Databricks but soft-deleting DB record
[INFO] Soft-deleted pipeline 'analytics_pipeline' from database
```

### No Orphaned Pipelines

```
[INFO] No orphaned pipelines found for share pack {share_pack_id}
```

## Safety Features

1. **Non-Destructive by Default**: Only deletes from Databricks if asset truly unused
2. **Database Integrity**: Always soft-deletes DB records (preserves history via SCD2)
3. **Error Handling**: Cleanup failures don't fail the entire provisioning
4. **Audit Trail**: All deletions logged with reasons
5. **Cross-Share Protection**: Checks ALL shares before deleting

## YAML Examples

### Example 1: Remove Assets (Triggers Cleanup)

```yaml
sharepack:
  name: "cleanup_old_data"
  strategy: "UPDATE"

shares:
  - name: analytics_share
    description: "Analytics data"

    # Remove old assets - pipelines processing these will be cleaned up
    share_assets_to_remove:
      - warehouse.analytics.deprecated_2023_q1
      - warehouse.analytics.deprecated_2023_q2
```

### Example 2: Declarative Approach (Implicit Removal)

```yaml
sharepack:
  name: "update_share_assets"
  strategy: "UPDATE"

shares:
  - name: sales_share

    # Declarative: Only these assets should exist
    # Any asset not in this list will be removed (triggers cleanup)
    share_assets:
      - main.sales.transactions  # Keep
      - main.sales.customers     # Keep
      # main.sales.old_reports removed (was in share before)
```

## Monitoring

### Database Queries

**Check soft-deleted pipelines:**
```sql
SELECT pipeline_name, source_table, is_deleted, effective_to, change_reason
FROM deltashare.pipelines
WHERE is_deleted = true
ORDER BY effective_to DESC;
```

**Find orphaned pipelines before cleanup runs:**
```sql
SELECT p.pipeline_name, p.source_table, p.share_id, s.share_name
FROM deltashare.pipelines p
JOIN deltashare.shares s ON p.share_id = s.share_id
WHERE p.is_current = true
  AND p.is_deleted = false
  AND NOT (p.source_table = ANY(string_to_array(s.share_assets::text, ',')::text[]));
```

## Troubleshooting

### Pipeline Not Deleted When Expected

**Symptom**: Pipeline kept in Databricks even though you removed the asset

**Possible Causes**:
1. Asset exists in another share → Check with:
   ```sql
   SELECT share_name, share_assets
   FROM deltashare.shares
   WHERE share_assets LIKE '%your_asset%'
   AND is_current = true
   AND is_deleted = false;
   ```

2. Pipeline's source_asset doesn't match removed asset exactly (case-sensitive)

### Cleanup Failed (Non-Fatal)

**Symptom**: Warning log: "Pipeline cleanup failed (non-fatal)"

**Impact**: Provisioning continues, but pipelines not cleaned up

**Action**:
1. Check logs for specific error
2. Run cleanup manually or on next provision
3. Orphaned pipelines remain in Databricks but marked in DB

## Best Practices

1. **Review Before Removing**: Check which pipelines will be affected
   ```bash
   # List pipelines using specific asset
   grep "source_asset.*your_asset" pipelines_config.yaml
   ```

2. **Gradual Removal**: Remove assets from one share at a time for better tracking

3. **Monitor Logs**: Always review cleanup logs to verify expected behavior

4. **Database Audit**: Periodically review soft-deleted pipelines to ensure consistency

## Related Documentation

- [Share Assets Explicit Approach](../sharepack_templates/SHARE_ASSETS_EXPLICIT_APPROACH.md)
- [Pipeline DELETE Strategy](../sharepack_templates/README.md#delete-strategy)
- [SCD Type 2 Database Design](./SCD2_DESIGN.md)
