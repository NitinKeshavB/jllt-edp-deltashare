# SharePack Latest Features & Updates

## Overview

This document summarizes all the latest enhancements to the SharePack workflow system.

---

## 🎯 Key Updates

### 1. Enhanced Metadata Validation
- ✅ Email validation with typo detection (gmail.co → gmail.com)
- ✅ Support for AD group names (without @ symbol)
- ✅ Workspace URL reachability check (HTTP HEAD request)
- ✅ ServiceNow ticket validation (required field)
- ✅ All governance fields validated

### 2. UPDATE Strategy Enhancements
- ✅ Immutable field protection (source_asset, scd_type)
- ✅ Key column validation against source table schema
- ✅ Only updates existing resources (no creation)
- ✅ Comprehensive schedule management (add, update, remove)
- ✅ Proper error handling and rollback

### 3. Advanced Schedule Management
- ✅ **Add** new schedules to pipelines
- ✅ **Update** existing schedules (cron, timezone)
- ✅ **Remove** schedules with explicit action
- ✅ No changes option (omit schedule field)

### 4. Intelligent Retry Logic
- ✅ Retry ONLY on timeout/network errors
- ✅ Fail immediately on validation errors
- ✅ One retry after 10 minutes
- ✅ Clear failure messages

### 5. Share Pack Deduplication
- ✅ Reuse same share_pack_id for re-runs
- ✅ Based on: requestor + business_line + project_name
- ✅ Enables idempotent re-provisioning

---

## 📋 Quick Reference

### YAML Files Created
1. **[sample_sharepack_NEW_strategy.yaml](sample_sharepack_NEW_strategy.yaml)**
   - Complete NEW strategy example
   - Shows all metadata fields
   - Multiple recipients (D2O, D2D)
   - Multiple shares with pipelines
   - Schedule examples

2. **[sample_sharepack_UPDATE_strategy.yaml](sample_sharepack_UPDATE_strategy.yaml)**
   - Complete UPDATE strategy example
   - Shows immutable field restrictions
   - Schedule management examples
   - Error scenario documentation

### Documentation Created
1. **[excel_format_guide.md](excel_format_guide.md)**
   - Complete Excel format specification
   - 4 sheets: metadata, recipient, share, pipelines
   - Examples for NEW and UPDATE strategies
   - Tags and cron format

2. **[schedule_management_guide.md](schedule_management_guide.md)**
   - Schedule operations (add, update, remove)
   - Cron expression reference
   - Complete examples
   - Error scenarios

3. **[SHAREPACK_LATEST_FEATURES.md](SHAREPACK_LATEST_FEATURES.md)** (this file)
   - Summary of all features
   - Quick reference

---

## 🔒 Immutable vs Mutable Fields (UPDATE Strategy)

### ❌ IMMUTABLE (Cannot Change)
| Field | Reason | Error if Changed |
|-------|--------|------------------|
| `source_asset` | Core pipeline identity | "Cannot change source_asset for existing pipeline" |
| `scd_type` | Fundamental data handling | "Cannot change scd_type for existing pipeline" |

### ✅ MUTABLE (Can Update)
| Field | Validation |
|-------|------------|
| `target_asset` | Target table name |
| `key_columns` | Must exist in source table |
| `notifications` | Email list |
| `serverless` | Boolean |
| `tags` | Key-value pairs |
| `schedule` | Cron, timezone, or remove |

---

## 📅 Schedule Management Examples

### Add New Schedule
```yaml
schedule:
  cron: "0 0 0 * * ?"
  timezone: "UTC"
```

### Update Existing Schedule
```yaml
schedule:
  cron: "0 0 6 * * ?"  # Changed time
  timezone: "America/New_York"  # Changed timezone
```

### Remove Schedule
```yaml
schedule:
  action: "remove"
```

### No Changes
```yaml
# Omit schedule field entirely
```

---

## 🔄 Retry Logic

### Retryable Errors
- ✅ Timeout errors
- ✅ Connection errors
- ✅ HTTP 503/504
- ✅ Rate limiting (429)

**Action**: Retry once after 10 minutes

### Non-Retryable Errors
- ❌ ValueError (validation, immutable changes)
- ❌ RuntimeError (pipeline/share failures)
- ❌ PermissionError
- ❌ Invalid key_columns

**Action**: Fail immediately, no retry

---

## 📊 Validation Summary

### Metadata
| Field | Validation |
|-------|------------|
| `requestor` | Valid email or AD group |
| `workspace_url` | HTTPS, reachable via HEAD request |
| `servicenow` | Required (ticket or link) |
| `approver_status` | One of: approved, declined, request_more_info, pending |
| `strategy` | NEW or UPDATE |

### Share Assets
| Validation | Description |
|------------|-------------|
| 3-part names | catalog.schema.table |
| Pipeline required | Every asset must have matching pipeline |
| source_asset match | Pipeline source_asset must match share_asset |

### Pipelines
| Field | Validation |
|-------|------------|
| `key_columns` | Must exist in source table (case-insensitive) |
| `source_asset` | Immutable in UPDATE strategy |
| `scd_type` | Immutable in UPDATE strategy |
| `schedule_cron` | Quartz 6-field format |
| `schedule_timezone` | Valid IANA timezone |

---

## 🚀 Workflow Flow

### NEW Strategy
```
Upload → Parse → Validate Metadata → Validate Assets → Validate Pipelines
  → Store in DB → Enqueue → Process:
    1. Create Recipients
    2. Create Shares
    3. Attach Recipients to Shares
    4. Create Pipelines (with validations)
    5. Create Schedules
  → Success: COMPLETED | Failure: FAILED (retry if timeout)
```

### UPDATE Strategy
```
Upload → Parse → Validate Metadata → Validate Assets → Validate Pipelines
  → Check Immutable Fields → Validate Key Columns → Store in DB → Enqueue
  → Process:
    1. Update Recipients (if changed)
    2. Update Share Assets (if changed)
    3. Update Share Permissions (if changed)
    4. Update Pipeline Config (mutable fields only)
    5. Manage Schedules (add/update/remove)
  → Success: COMPLETED | Failure: FAILED + Rollback (retry if timeout)
```

---

## 🎭 Example Scenarios

### Scenario 1: Create New Finance Share Pack (NEW Strategy)
```yaml
metadata:
  strategy: NEW
  requestor: finance-team@company.com
  business_line: Finance
  project_name: Q1_Reports

recipient:
  - name: external_partner
    type: D2O
    recipient: partner@external.com

share:
  - name: finance_share
    share_assets:
      - catalog.schema.transactions
    pipelines:
      - name_prefix: txn_pipeline
        source_asset: catalog.schema.transactions
        scd_type: "2"
        key_columns: "txn_id,date"
        schedule:
          cron: "0 0 2 * * ?"
          timezone: "UTC"
```

**Result**: Creates recipient, share, pipeline, schedule

---

### Scenario 2: Update Pipeline Schedule (UPDATE Strategy)
```yaml
metadata:
  strategy: UPDATE
  # ... same metadata ...

share:
  - name: finance_share  # Must exist
    pipelines:
      - name_prefix: txn_pipeline  # Must exist
        source_asset: catalog.schema.transactions  # Must match existing
        scd_type: "2"  # Must match existing
        target_asset: transactions_v2  # ✅ Updated
        key_columns: "txn_id,date,customer_id"  # ✅ Added column (validated)
        schedule:
          cron: "0 0 6 * * ?"  # ✅ Changed from 2 AM to 6 AM
          timezone: "America/New_York"  # ✅ Changed timezone
```

**Result**: Updates target_asset, key_columns, schedule

---

### Scenario 3: Remove Schedule (UPDATE Strategy)
```yaml
metadata:
  strategy: UPDATE

share:
  - name: finance_share
    pipelines:
      - name_prefix: txn_pipeline
        source_asset: catalog.schema.transactions
        scd_type: "2"
        schedule:
          action: "remove"  # ✅ Deletes schedule
```

**Result**: Schedule removed, pipeline remains

---

### Scenario 4: Invalid Update - Change source_asset ❌
```yaml
metadata:
  strategy: UPDATE

share:
  - name: finance_share
    pipelines:
      - name_prefix: txn_pipeline
        source_asset: catalog.schema.NEW_TABLE  # ❌ Changed!
        scd_type: "2"
```

**Result**:
- Error: "Cannot change source_asset for existing pipeline"
- Status: FAILED
- No retry (non-retryable error)

---

### Scenario 5: Invalid Update - Invalid key_columns ❌
```yaml
metadata:
  strategy: UPDATE

share:
  - name: finance_share
    pipelines:
      - name_prefix: txn_pipeline
        source_asset: catalog.schema.transactions
        scd_type: "2"
        key_columns: "invalid_column"  # ❌ Doesn't exist in table
```

**Result**:
- Error: "Invalid key_columns: column 'invalid_column' does not exist in source table"
- Status: FAILED
- No retry (non-retryable error)

---

## 🔧 API Endpoints

### Upload Share Pack
```http
POST /workflow/sharepack/upload_and_validate
Content-Type: multipart/form-data
X-Workspace-URL: https://adb-123.azuredatabricks.net

File: sharepack.yaml or sharepack.xlsx
```

**Response (202 Accepted)**:
```json
{
  "Message": "Share pack uploaded successfully and queued for provisioning",
  "SharePackId": "uuid",
  "SharePackName": "SharePack_requestor_businessline_project",
  "Status": "IN_PROGRESS",
  "ValidationErrors": [],
  "ValidationWarnings": []
}
```

### Check Status
```http
GET /workflow/sharepack/{share_pack_id}
```

**Response**:
```json
{
  "SharePackId": "uuid",
  "SharePackName": "SharePack_requestor_businessline_project",
  "Status": "COMPLETED",  // or "IN_PROGRESS", "FAILED"
  "Strategy": "UPDATE",
  "ProvisioningStatus": "Step 5/5: Schedules updated",
  "ErrorMessage": "",
  "RequestedBy": "john.doe@company.com",
  "CreatedAt": "2025-01-15T10:30:00Z",
  "LastUpdated": "2025-01-15T10:35:00Z"
}
```

---

## 📈 Status Messages

### Success
- `Status: "COMPLETED"`
- `ProvisioningStatus: "All resources provisioned successfully"`

### In Progress
- `Status: "IN_PROGRESS"`
- `ProvisioningStatus: "Step 3/5: Creating pipelines..."`

### Failed - Non-Retryable
- `Status: "FAILED"`
- `ProvisioningStatus: "Non-retryable error: ValueError"`
- `ErrorMessage: "Cannot change source_asset for existing pipeline..."`

### Failed - After Retry
- `Status: "FAILED"`
- `ProvisioningStatus: "Retried failed request and stopping"`
- `ErrorMessage: "Provisioning failed after 2 attempts. Last error: ReadTimeout..."`

---

## 🎓 Best Practices

1. **Use NEW for Initial Setup**
   - Create all resources first
   - Validate everything works
   - Then use UPDATE for modifications

2. **Test Validation Locally**
   - Check email formats
   - Verify workspace URL is reachable
   - Validate table/column names

3. **Plan Updates Carefully**
   - Remember: source_asset and scd_type are immutable
   - Validate key_columns exist before uploading
   - Test schedule cron expressions

4. **Use Deduplication**
   - Keep same requestor + business_line + project_name
   - Enables re-running failed provisions
   - Maintains audit trail

5. **Monitor Status**
   - Poll `/workflow/sharepack/{id}` endpoint
   - Check logs for detailed error messages
   - Review rollback messages if failed

6. **Handle Failures**
   - Non-retryable: Fix YAML and re-upload
   - Retryable: Wait for automatic retry (10 min)
   - After failure: Fix issue and re-upload (same share_pack_id reused)

---

## 📚 Additional Resources

- **Sample YAML Files**: See `sample_sharepack_NEW_strategy.yaml` and `sample_sharepack_UPDATE_strategy.yaml`
- **Excel Guide**: See `excel_format_guide.md` for Excel format
- **Schedule Guide**: See `schedule_management_guide.md` for schedule operations
- **Cron Reference**: See schedule guide for cron expression examples

---

## 🆕 What's New Summary

1. **Metadata Validation** - Enhanced email validation, workspace reachability
2. **Immutable Fields** - source_asset and scd_type protected in UPDATE
3. **Key Column Validation** - Validated against actual table schema
4. **Schedule Management** - Add, update, remove operations
5. **Smart Retry** - Only retries timeouts, fails fast on validation errors
6. **Deduplication** - Reuse share_pack_id for idempotent re-runs
7. **Better Error Messages** - Clear distinction between retryable and non-retryable
8. **Rollback Support** - All changes rolled back on any failure

---

## 🔗 Quick Links

- NEW Strategy YAML: [sample_sharepack_NEW_strategy.yaml](sample_sharepack_NEW_strategy.yaml)
- UPDATE Strategy YAML: [sample_sharepack_UPDATE_strategy.yaml](sample_sharepack_UPDATE_strategy.yaml)
- Excel Format Guide: [excel_format_guide.md](excel_format_guide.md)
- Schedule Management: [schedule_management_guide.md](schedule_management_guide.md)

---

**Last Updated**: 2025-02-03
**Version**: 2.0
**Status**: Production Ready ✅
