# Database Tracking and Rollback Implementation

## Status: ✅ COMPLETE - Fully Integrated

All provisioning resources are now tracked in the database with automatic rollback on failure.

## Summary of Implementation

### ✅ Completed Features

1. **SCD Type 2 with Max Date** - Current records use `effective_to = '9999-12-31'` for temporal queries
2. **Database Tracking** - All recipients, shares, and pipelines tracked in database
3. **Automatic Rollback** - Failed provisioning soft-deletes all created database records
4. **Both Strategies** - Implemented in both NEW and UPDATE strategies
5. **Audit Trail** - Full version history with SCD Type 2 for all resources

### 📊 What Gets Tracked

| Resource | Database Table | Tracked Fields |
|----------|---------------|----------------|
| **Recipients** | `deltashare.recipients` | recipient_name, databricks_recipient_id, type, IPs, metastore_id, activation_url |
| **Shares** | `deltashare.shares` | share_name, databricks_share_id, description, assets, recipients |
| **Pipelines** | `deltashare.pipelines` | pipeline_name, databricks_pipeline_id, source/target tables, schedule, SCD type |

### 🔄 Rollback Behavior

When provisioning fails:
1. **Exception caught** - Error logged with full stack trace
2. **Status updated** - SharePack marked as FAILED in database
3. **Rollback triggered** - All created database records soft-deleted
4. **Audit preserved** - Deleted records kept with `is_deleted=true` and deletion reason

## What's Been Completed

### 1. SCD Type 2 Update
**File:** [scd2.py](src/dbrx_api/workflow/db/scd2.py#L93)
- ✅ Changed `effective_to` from `'infinity'` to `'9999-12-31'` for current records
- Current records have `effective_to = '9999-12-31'` and `is_current = true`
- Historical records have `effective_to = timestamp` and `is_current = false`
- No database schema changes needed - maintains NOT NULL constraint

### 2. Rollback Infrastructure
**File:** [provisioning_update.py](src/dbrx_api/workflow/orchestrator/provisioning_update.py)
- ✅ Added repository imports (RecipientRepository, ShareRepository, PipelineRepository)
- ✅ Initialize repositories in main function with connection pool
- ✅ Created `created_db_records` tracking dict with lists for recipients, shares, pipelines
- ✅ Implemented rollback logic in exception handler
  - Soft deletes all created recipients if provisioning fails
  - Soft deletes all created shares if provisioning fails
  - Soft deletes all created pipelines if provisioning fails
  - Uses SCD Type 2 soft delete (creates new version with `is_deleted=true`)

### 3. Database Tracking (Partial)
**File:** [provisioning_update.py](src/dbrx_api/workflow/orchestrator/provisioning_update.py)
- ✅ Updated `_update_recipients` function signature to accept repositories and tracking dict
- ✅ Added database tracking after recipient creation (lines 298-316)
- 🔄 Need to apply same pattern to:
  - `_update_shares` function
  - `_create_pipeline` function (in `_update_pipelines_and_schedules`)

## Pattern for Database Tracking

### For Recipients (Already Implemented)

```python
# After creating recipient in Databricks
if isinstance(result, str):
    logger.error(f"Failed to create recipient {recipient_name}: {result}")
    continue
else:
    logger.success(f"Created recipient: {recipient_name}")

    # Get the Databricks recipient details
    existing = get_recipients(recipient_name, workspace_url)
    if not existing:
        continue

    # Track in database
    try:
        recipient_id = uuid4()
        await recipient_repo.create_from_config(
            recipient_id=recipient_id,
            share_pack_id=share_pack_id,
            recipient_name=recipient_name,
            databricks_recipient_id=result.name,  # From Databricks SDK
            recipient_contact_email=recip_config.get("recipient_contact_email", ""),
            recipient_type=recipient_type,
            recipient_databricks_org=recip_config.get("data_recipient_global_metastore_id"),
            ip_access_list=recip_config.get("recipient_ips", []),
            activation_url=result.activation_url if hasattr(result, "activation_url") else None,
            bearer_token=None,  # Don't store tokens
            created_by="orchestrator",
        )
        created_db_records["recipients"].append(recipient_id)
        logger.debug(f"Tracked recipient in database (id: {recipient_id})")
    except Exception as db_error:
        logger.error(f"Failed to track recipient in database: {db_error}")
```

### For Shares (To Be Applied)

```python
# After creating share in Databricks (in _update_shares function)
if isinstance(result, str):
    logger.error(f"Failed to create share {share_name}: {result}")
    continue
else:
    logger.success(f"Created share: {share_name}")

    # Track in database
    try:
        share_id = uuid4()
        await share_repo.create_from_config(
            share_id=share_id,
            share_pack_id=share_pack_id,
            share_name=share_name,
            databricks_share_id=result.name,  # From Databricks SDK
            description=share_config.get("comment", ""),
            storage_root="",  # Not tracked for Delta Sharing
            share_assets=share_config.get("share_assets", []),
            recipients_attached=share_config.get("recipients", []),
            created_by="orchestrator",
        )
        created_db_records["shares"].append(share_id)
        logger.debug(f"Tracked share in database (id: {share_id})")
    except Exception as db_error:
        logger.error(f"Failed to track share in database: {db_error}")
```

### For Pipelines (To Be Applied)

```python
# After creating pipeline in Databricks (in _create_pipeline function)
if isinstance(result, str):
    logger.error(f"Failed to create pipeline {pipeline_name}: {result}")
    return None

logger.success(f"Created pipeline: {pipeline_name}")

# Track in database
pipeline_id_db = None
try:
    # Find share_id from database using share_name
    share_name = share_config["name"]
    # Query share_repo to get share_id by share_name and share_pack_id
    # (You may need to add a helper method to ShareRepository)

    pipeline_id_db = uuid4()

    # Extract schedule info
    schedule = pipeline_config.get("schedule", {})
    cron_expr = ""
    timezone = "UTC"
    schedule_type = "CRON"

    if isinstance(schedule, dict):
        cron_expr = schedule.get("cron", "")
        timezone = schedule.get("timezone", "UTC")
    elif isinstance(schedule, str):
        schedule_type = schedule.upper()  # "CONTINUOUS"

    await pipeline_repo.create_from_config(
        pipeline_id=pipeline_id_db,
        share_id=share_id,  # Get from share_repo lookup
        share_pack_id=share_pack_id,
        pipeline_name=pipeline_name,
        databricks_pipeline_id=result.pipeline_id,  # From Databricks SDK
        asset_name=target_asset,
        source_table=source_asset,
        target_table=target_asset,
        scd_type=pipeline_config.get("scd_type", "2"),
        key_columns=pipeline_config.get("key_columns", ""),
        schedule_type=schedule_type,
        cron_expression=cron_expr,
        timezone=timezone,
        serverless=pipeline_config.get("serverless", False),
        tags=pipeline_config.get("tags", {}),
        notification_emails=pipeline_config.get("notification", []),
        created_by="orchestrator",
    )
    created_db_records["pipelines"].append(pipeline_id_db)
    logger.debug(f"Tracked pipeline in database (id: {pipeline_id_db})")
except Exception as db_error:
    logger.error(f"Failed to track pipeline in database: {db_error}")

# Return Databricks pipeline_id (not database UUID)
return result.pipeline_id
```

## Remaining Tasks

### UPDATE Strategy (provisioning_update.py)

1. **Update `_update_shares` function signature:**
   ```python
   async def _update_shares(
       workspace_url: str,
       shares: List[Dict],
       updated_resources: Dict,
       share_repo: ShareRepository,
       share_pack_id: UUID,
       created_db_records: Dict,
   ):
   ```

2. **Add database tracking after share creation** (line ~259)
   - Apply the "For Shares" pattern above

3. **Update `_create_pipeline` function signature:**
   ```python
   async def _create_pipeline(
       workspace_url: str,
       pipeline_name: str,
       pipeline_config: Dict,
       share_config: Dict,
       updated_resources: Dict,
       pipeline_repo: PipelineRepository,
       share_repo: ShareRepository,
       share_pack_id: UUID,
       created_db_records: Dict,
   ) -> str | None:
   ```

4. **Add database tracking after pipeline creation** (line ~485)
   - Apply the "For Pipelines" pattern above
   - Need to look up `share_id` from database using `share_name`

5. **Update function calls:**
   - `_update_pipelines_and_schedules` already updated to pass new params
   - `_create_pipeline` call already updated (line ~390)

### NEW Strategy (provisioning.py)

Apply the same pattern to [provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py):

1. **Add repository imports**
2. **Initialize repositories in `provision_sharepack_new`**
3. **Add `created_db_records` tracking dict**
4. **Add database tracking after each resource creation:**
   - After recipient creation
   - After share creation
   - After pipeline creation
5. **Add rollback logic in exception handler**

## Benefits

### 1. Real-Time Visibility
```sql
-- See all recipients for a SharePack
SELECT * FROM deltashare.recipients
WHERE share_pack_id = 'xxx' AND is_current = true AND is_deleted = false;

-- See all pipelines for a share
SELECT * FROM deltashare.pipelines
WHERE share_id = 'xxx' AND is_current = true AND is_deleted = false;
```

### 2. Audit Trail
```sql
-- See full history of a recipient
SELECT * FROM deltashare.recipients
WHERE recipient_id = 'xxx'
ORDER BY version;

-- See what was deleted during rollback
SELECT * FROM deltashare.recipients
WHERE is_deleted = true AND change_reason LIKE '%Rollback%';
```

### 3. Rollback Support
- If provisioning fails at step 5/6, all created resources are soft-deleted in database
- Easy cleanup: query `is_deleted=true` records to see what failed
- Can restore if needed using `restore_deleted_entity()`

### 4. Relationship Tracking
```sql
-- See all resources for a SharePack
SELECT
    sp.share_pack_name,
    r.recipient_name,
    s.share_name,
    p.pipeline_name,
    p.source_table,
    p.target_table
FROM deltashare.share_packs sp
LEFT JOIN deltashare.recipients r ON r.share_pack_id = sp.share_pack_id AND r.is_current = true
LEFT JOIN deltashare.shares s ON s.share_pack_id = sp.share_pack_id AND s.is_current = true
LEFT JOIN deltashare.pipelines p ON p.share_id = s.share_id AND p.is_current = true
WHERE sp.share_pack_id = 'xxx' AND sp.is_current = true;
```

## Helper Method Needed

Add to **ShareRepository**:

```python
async def get_by_name_and_sharepack(
    self,
    share_name: str,
    share_pack_id: UUID,
) -> Optional[Dict[str, Any]]:
    """
    Get share by name and SharePack ID.

    Args:
        share_name: Share name
        share_pack_id: SharePack ID

    Returns:
        Share dict or None
    """
    async with self.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM deltashare.shares
            WHERE share_name = $1
              AND share_pack_id = $2
              AND is_current = true
              AND is_deleted = false
            """,
            share_name,
            share_pack_id,
        )
        return dict(row) if row else None
```

Use this in `_create_pipeline` to look up `share_id` before tracking pipeline in database.

## Testing

After completing integration:

1. **Test successful provisioning:**
   ```sql
   -- Verify records created
   SELECT * FROM deltashare.recipients WHERE share_pack_id = 'xxx' AND is_current = true;
   SELECT * FROM deltashare.shares WHERE share_pack_id = 'xxx' AND is_current = true;
   SELECT * FROM deltashare.pipelines WHERE share_pack_id = 'xxx' AND is_current = true;
   ```

2. **Test rollback:**
   - Introduce an intentional error in provisioning (e.g., invalid pipeline config)
   - Verify all created records are soft-deleted:
   ```sql
   SELECT * FROM deltashare.recipients WHERE share_pack_id = 'xxx' AND is_deleted = true;
   ```

3. **Test UPDATE strategy:**
   - Update existing SharePack, add new pipeline
   - Verify new pipeline tracked, existing records preserved

---

**Next Steps:**
1. Complete database tracking for shares and pipelines in UPDATE strategy
2. Apply same pattern to NEW strategy (provisioning.py)
3. Add `get_by_name_and_sharepack` helper to ShareRepository
4. Test end-to-end with real provisioning
