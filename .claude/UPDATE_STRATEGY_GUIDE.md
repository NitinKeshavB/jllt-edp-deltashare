# UPDATE Strategy - Selective Resource Updates

## Overview

The UPDATE strategy allows you to selectively update specific resources (recipients, shares, pipelines, schedules) without touching others. You only pass what you want to update in the YAML.

## Key Features

✅ **Selective Updates** - Only update what's in the YAML
✅ **Idempotent** - Safe to run multiple times
✅ **Non-Destructive** - Won't delete resources not mentioned
✅ **Flexible** - Update any combination of resources

## How It Works

### 1. Detection Phase
System detects which sections are present in your YAML:
```python
has_recipients = "recipient" in config
has_shares = "share" in config
has_pipelines = share has "pipelines" section
```

### 2. Update Phase
For each present section:
- Fetch existing resources
- Compare with new config
- Update only what changed
- Skip resources that don't exist (logs warning)

### 3. Idempotent Operations
All update operations are safe to repeat:
- Add IPs → No error if already present
- Add data objects → No error if already in share
- Update schedule → Only updates if different

## Usage Examples

### Example 1: Update Only Schedules

Change pipeline schedules without touching anything else:

```yaml
metadata:
  strategy: UPDATE  # Use UPDATE strategy
  workspace_url: "https://adb-xxx.azuredatabricks.net"

# Only include what you want to update
share:
  - name: sales_share
    pipelines:
      - name_prefix: sales_daily_sync
        schedule:
          cron: "0 0 3 * * ?"  # Changed from 2 AM to 3 AM
          timezone: "America/New_York"
```

**Result:**
- ✓ Updates schedule for `sales_daily_sync` pipeline
- ✓ Leaves all other resources unchanged
- ✓ Recipients, shares, data objects untouched

### Example 2: Update Only Recipient IPs

Add or remove IPs from recipient without changing anything else:

```yaml
metadata:
  strategy: UPDATE

recipient:
  - name: external_partner
    type: D2O
    recipient_ips:
      - 203.0.113.0/24    # Existing
      - 198.51.100.0/24   # New - will be added
      # 192.0.2.0/24 removed - will be revoked
```

**Result:**
- ✓ Adds new IPs: `198.51.100.0/24`
- ✓ Removes missing IPs: `192.0.2.0/24`
- ✓ Keeps existing IPs: `203.0.113.0/24`
- ✓ Pipelines, shares, schedules untouched

### Example 3: Update Multiple Resources

Update recipients + schedules in one go:

```yaml
metadata:
  strategy: UPDATE

recipient:
  - name: external_partner
    type: D2O
    recipient_ips:
      - 203.0.113.0/24
      - 198.51.100.0/24  # Added
    comment: "Updated description"  # Changed

share:
  - name: sales_share
    pipelines:
      - name_prefix: sales_daily_sync
        schedule:
          cron: "0 0 3 * * ?"
          timezone: "UTC"  # Changed timezone
```

**Result:**
- ✓ Updates recipient IPs and description
- ✓ Updates pipeline schedule and timezone
- ✓ Everything else unchanged

### Example 4: Add New Data Objects to Share

Add more assets to an existing share:

```yaml
metadata:
  strategy: UPDATE

share:
  - name: sales_share
    share_assets:
      - catalog.sales.daily_sales    # Existing
      - catalog.sales.weekly_summary # New - will be added
      - catalog.sales.monthly_report # New - will be added
```

**Result:**
- ✓ Adds new assets to share
- ✓ Existing assets remain
- ✓ Recipients, pipelines unchanged

### Example 5: Update Share Permissions

Add recipients to an existing share:

```yaml
metadata:
  strategy: UPDATE

share:
  - name: sales_share
    recipients:
      - external_partner      # Existing
      - internal_team         # New - will be granted access
      - compliance_auditor    # New - will be granted access
```

**Result:**
- ✓ Grants access to new recipients
- ✓ Existing recipients keep access
- ✓ Share data objects unchanged

## What Gets Updated

### Recipients

| Field | Update Behavior |
|-------|----------------|
| `comment` | Updates description if changed |
| `recipient_ips` | Adds new IPs, removes missing IPs |
| `token_expiry` | Not yet supported |
| `token_rotation` | Not yet supported |

**Note:** Cannot update recipient `type` (D2D ↔ D2O) or `recipient_databricks_org`

### Shares

| Field | Update Behavior |
|-------|----------------|
| `share_assets` | Adds new assets (doesn't remove existing) |
| `recipients` | Grants access to new recipients (doesn't revoke existing) |
| `comment` | Not yet supported |

### Pipelines

| Field | Update Behavior |
|-------|----------------|
| `schedule.cron` | Updates cron expression if changed |
| `schedule.timezone` | Updates timezone if changed |
| Pipeline config | Not yet supported (catalog, schema, source/target) |
| `tags` | Not yet supported |
| `notification` | Not yet supported |

**Note:** Schedule updates require pipeline to exist

### Schedules

| Field | Update Behavior |
|-------|----------------|
| `cron` | Updates cron expression |
| `timezone` | Updates timezone |
| `paused` | Not yet supported |
| Continuous schedules | Not yet supported |

## Update Steps

The UPDATE strategy runs through 6 steps:

1. **Initialize** - Parse config, detect sections
2. **Update Recipients** - IP lists, descriptions
3. **Update Shares** - Verify shares exist
4. **Update Data Objects** - Add new assets to shares
5. **Update Permissions** - Grant access to new recipients
6. **Update Pipelines & Schedules** - Cron expressions, timezones

Each step is logged for visibility.

## Behavior Details

### Resource Not Found

If a resource doesn't exist, UPDATE logs a warning and skips it:

```
⚠️  Pipeline 'sales_daily_sync' not found - cannot update (use NEW strategy to create)
```

**This is intentional** - UPDATE doesn't create resources, only updates existing ones.

### Idempotent Operations

All APIs are idempotent - safe to run multiple times:

```yaml
# Run this twice - no errors
recipient:
  - name: external_partner
    recipient_ips:
      - 203.0.113.0/24  # Already present - no error
```

### Partial Updates

If some updates fail, others continue:
```
✓ Updated recipient: external_partner (IPs)
❌ Failed to update pipeline: sales_sync (not found)
✓ Updated schedule: daily_batch (timezone)
```

The SharePack completes with partial updates applied.

### Non-Destructive

UPDATE **never removes** resources not mentioned:

```yaml
# Original: 3 assets
share_assets:
  - catalog.sales.daily
  - catalog.sales.weekly
  - catalog.sales.monthly

# Update: Only mention 1 asset
share_assets:
  - catalog.sales.daily

# Result: All 3 assets remain (daily added again, others untouched)
```

## Comparison: NEW vs UPDATE

| Aspect | NEW Strategy | UPDATE Strategy |
|--------|-------------|-----------------|
| **Purpose** | Create new resources | Update existing resources |
| **If exists** | Skips (idempotent) | Updates if changed |
| **If missing** | Creates | Logs warning, skips |
| **Scope** | All resources | Only specified resources |
| **YAML** | Full config required | Minimal (only changes) |

## Common Use Cases

### Use Case 1: Schedule Adjustments

**Scenario:** Change pipeline run times for all pipelines

**YAML:**
```yaml
strategy: UPDATE
share:
  - name: sales_share
    pipelines:
      - name_prefix: pipeline1
        schedule: {cron: "0 0 2 * * ?", timezone: "UTC"}
      - name_prefix: pipeline2
        schedule: {cron: "0 0 3 * * ?", timezone: "UTC"}
```

### Use Case 2: IP Whitelist Changes

**Scenario:** External partner changed IPs

**YAML:**
```yaml
strategy: UPDATE
recipient:
  - name: external_partner
    recipient_ips: [203.0.113.0/24, 198.51.100.0/24]
```

### Use Case 3: Add More Data to Share

**Scenario:** Share additional tables with existing recipients

**YAML:**
```yaml
strategy: UPDATE
share:
  - name: sales_share
    share_assets:
      - catalog.sales.new_table1
      - catalog.sales.new_table2
```

### Use Case 4: Grant Access to New Recipients

**Scenario:** New team needs access to existing share

**YAML:**
```yaml
strategy: UPDATE
share:
  - name: sales_share
    recipients:
      - new_team_recipient
```

## Error Handling

### Resource Not Found
```
⚠️  Resource 'xyz' not found - cannot update
Continue with other resources...
✓ Other updates completed successfully
```

### Permission Denied
```
❌ Permission denied to update recipient 'xyz'
Continue with other resources...
```

### API Error
```
❌ Failed to update schedule: API error
Continue with other resources...
```

## Limitations (Current)

❌ Cannot update pipeline configuration (source, target, catalog, schema)
❌ Cannot update recipient type (D2D ↔ D2O)
❌ Cannot remove data objects from shares
❌ Cannot revoke recipient access from shares
❌ Cannot update continuous schedules
❌ Cannot pause/unpause schedules

**Workaround:** Use DELETE strategy (future) or manual cleanup + NEW strategy

## Logs Output

### Successful Update

```
INFO: Starting UPDATE strategy provisioning for xxx
INFO: Update scope: recipients=True, shares=True
INFO: Step 1/6: Initializing update
INFO: Step 2/6: Updating recipients
SUCCESS: Updated description for recipient: external_partner
SUCCESS: Added IPs to external_partner: {'198.51.100.0/24'}
INFO: Step 3/6: Updating shares
INFO: Step 4/6: Updating share data objects
SUCCESS: Updated data objects for share: sales_share
INFO: Step 5/6: Updating share permissions
SUCCESS: Updated permission: new_recipient → sales_share
INFO: Step 6/6: Updating pipelines and schedules
SUCCESS: Updated cron for sales_daily_sync: 0 0 3 * * ?
SUCCESS: Updated timezone for sales_daily_sync: UTC
SUCCESS: Share pack xxx updated successfully
INFO: Updated: 1 recipients, 1 shares, 1 pipelines, 2 schedules
```

### Partial Update (Some Resources Not Found)

```
WARNING: Pipeline 'xyz' not found - cannot update (use NEW strategy to create)
SUCCESS: Updated schedule for pipeline 'abc'
SUCCESS: Share pack completed with partial updates
```

## Best Practices

1. **Test in Dev First** - Upload to dev workspace before production
2. **Minimal YAMLs** - Only include what needs changing
3. **One Concern per Upload** - Update schedules separately from IPs
4. **Version Control** - Keep UPDATE YAMLs in git for history
5. **Log Review** - Check logs after update to verify changes
6. **Idempotent** - Safe to re-run if unsure

## Future Enhancements

Planned features:
- ✅ Pipeline configuration updates
- ✅ Remove data objects from shares
- ✅ Revoke recipient permissions
- ✅ Continuous schedule updates
- ✅ Pause/unpause schedules
- ✅ Bulk operations
- ✅ Rollback support

---

**Status:** ✅ Implemented
**Version:** 1.0
**Breaking Changes:** None (NEW strategy unchanged)
