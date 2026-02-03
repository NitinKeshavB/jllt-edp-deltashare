# SharePack v2.0 - Complete Guide

## 🎯 What's New in v2.0

Version 2.0 introduces critical improvements to the SharePack provisioning system:

### 1. **Explicit Source Assets**
- Each pipeline now has a `source_asset` field that explicitly specifies which share asset it processes
- No more extracting asset names from schedule keys (error-prone)
- Clear, unambiguous mapping between pipelines and assets

### 2. **Pipeline-Level Catalog/Schema Overrides**
- Pipelines can now override the default target catalog/schema
- Use `ext_catalog_name` and `ext_schema_name` at the pipeline level
- Priority: pipeline-level > share-level (delta_share)

### 3. **Timezone Support**
- All cron-based schedules now require timezone specification
- Supports standard timezone strings (America/New_York, Europe/London, UTC, etc.)
- Enables accurate multi-region scheduling

### 4. **Full Idempotency**
- System handles existing resources gracefully
- Re-running the same SharePack won't fail if resources already exist
- Logs existing resources and continues provisioning

---

## 📁 File Locations

### Sample Files Created

```
api_layer/
├── sample_sharepack_v2.yaml              # Complete YAML example
├── sample_sharepack_template_v2.md       # Excel structure documentation
└── excel_template/
    ├── 01_metadata.csv                   # Metadata sheet
    ├── 02_recipient.csv                  # Recipients sheet
    ├── 03_share.csv                      # Shares sheet
    ├── 04_pipelines.csv                  # Pipelines sheet (NEW STRUCTURE)
    ├── create_excel_template.py          # Script to generate .xlsx
    └── README.md                         # Quick reference
```

---

## 🚀 Quick Start

### Using YAML

1. **Copy and customize the sample:**
   ```bash
   cp api_layer/sample_sharepack_v2.yaml my_sharepack.yaml
   ```

2. **Edit the YAML file:**
   - Update metadata (workspace_url, requestor, etc.)
   - Define recipients (D2D or D2O)
   - Define shares and assets
   - **IMPORTANT:** For each pipeline, specify `source_asset` explicitly

3. **Upload via API:**
   ```bash
   curl -X POST "http://localhost:8000/workflow/sharepack/upload_and_validate" \
     -H "X-Workspace-URL: https://your-workspace.azuredatabricks.net" \
     -F "file=@my_sharepack.yaml"
   ```

### Using Excel

1. **Create Excel workbook from CSV files:**

   **Option A: Manual Import**
   - Open Excel, create 4 sheets: `metadata`, `recipient`, `share`, `pipelines`
   - Import each CSV file into its corresponding sheet
   - Save as `.xlsx`

   **Option B: Python Script (requires pandas)**
   ```bash
   cd api_layer/excel_template
   pip install pandas openpyxl
   python create_excel_template.py
   ```

2. **Customize the Excel file:**
   - Edit values in each sheet
   - Use `|` (pipe) separator for multi-value cells
   - Ensure each share_asset has a corresponding pipeline with matching source_asset

3. **Upload via API:**
   ```bash
   curl -X POST "http://localhost:8000/workflow/sharepack/upload_and_validate" \
     -H "X-Workspace-URL: https://your-workspace.azuredatabricks.net" \
     -F "file=@sample_sharepack_v2.xlsx"
   ```

---

## 📝 YAML Structure Example

### Pipeline with Explicit Source Asset

```yaml
share:
  - name: sales_analytics_share
    share_assets:
      - main_catalog.sales_schema.daily_sales
      - main_catalog.sales_schema.customer_orders

    delta_share:
      ext_catalog_name: analytics_prod    # Default catalog
      ext_schema_name: shared_sales       # Default schema

    pipelines:
      # Pipeline 1: Processes daily_sales asset
      - name_prefix: sales_daily_sync
        source_asset: main_catalog.sales_schema.daily_sales  # EXPLICIT
        scd_type: "2"
        key_columns: "sale_id,sale_date"
        schedule:
          cron: "0 0 2 * * ?"
          timezone: "America/New_York"

      # Pipeline 2: Processes customer_orders with CUSTOM target
      - name_prefix: sales_orders_sync
        source_asset: main_catalog.sales_schema.customer_orders  # EXPLICIT
        ext_catalog_name: custom_catalog   # OVERRIDE
        ext_schema_name: custom_schema     # OVERRIDE
        scd_type: "2"
        key_columns: "order_id,customer_id"
        schedule:
          cron: "0 0 */6 * * ?"
          timezone: "UTC"
```

### Continuous vs Cron Schedules

```yaml
pipelines:
  # Continuous (real-time streaming)
  - name_prefix: realtime_stream
    source_asset: catalog.schema.streaming_table
    schedule: "continuous"

  # Cron with timezone
  - name_prefix: hourly_batch
    source_asset: catalog.schema.batch_table
    schedule:
      cron: "0 0 * * * ?"
      timezone: "UTC"
```

---

## 📊 Excel Structure Example

### pipelines Sheet (Most Important Changes)

| share_name | name_prefix | **source_asset** | scd_type | key_columns | schedule_type | cron_expression | timezone | ext_catalog_name | ext_schema_name |
|------------|-------------|------------------|----------|-------------|---------------|-----------------|----------|------------------|-----------------|
| sales_analytics_share | sales_daily_sync | **main_catalog.sales_schema.daily_sales** | 2 | sale_id\|sale_date | cron | 0 0 2 * * ? | America/New_York | | |
| sales_analytics_share | revenue_sync | **main_catalog.sales_schema.revenue_summary** | 1 | | cron | 0 0 * * * ? | UTC | finance_catalog | revenue_reports |
| ops_share | health_stream | **ops_catalog.metrics.system_health** | 1 | | continuous | | | | |

**Key Points:**
- `source_asset` column explicitly states which share_asset the pipeline processes
- `ext_catalog_name` and `ext_schema_name` are optional overrides
- Use `|` separator for multi-value columns (key_columns, recipients, etc.)

---

## ✅ Validation Rules

### Critical Rules

1. **One pipeline per share_asset**: Every asset in `share_assets` must have exactly one pipeline
2. **source_asset must match**: Pipeline's `source_asset` must exist in the share's `share_assets` list
3. **3-part asset names**: All assets must be fully qualified: `catalog.schema.table`
4. **Cron requires timezone**: If schedule is cron-based, timezone is required
5. **D2D requires metastore**: If recipient type is D2D, `recipient_databricks_org` is required

### Example Validation

```yaml
share_assets:
  - catalog.schema.table1
  - catalog.schema.table2

pipelines:
  - name_prefix: pipeline1
    source_asset: catalog.schema.table1  # ✅ Matches
  - name_prefix: pipeline2
    source_asset: catalog.schema.table2  # ✅ Matches
  # ⚠️  Must have exactly 2 pipelines (one per asset)
```

---

## 🌍 Timezone Examples

| Region | Timezone String | Example Schedule |
|--------|----------------|------------------|
| US East Coast | America/New_York | 2 AM EST: `0 0 2 * * ?` |
| US West Coast | America/Los_Angeles | 2 AM PST: `0 0 2 * * ?` |
| London | Europe/London | 1 AM GMT: `0 0 1 * * ?` |
| Paris/Frankfurt | Europe/Paris | 1 AM CET: `0 0 1 * * ?` |
| India | Asia/Kolkata | 2 AM IST: `0 0 2 * * ?` |
| Tokyo | Asia/Tokyo | 2 AM JST: `0 0 2 * * ?` |
| Sydney | Australia/Sydney | 2 AM AEST: `0 0 2 * * ?` |
| UTC | UTC | 2 AM UTC: `0 0 2 * * ?` |

---

## 🔄 Migration from v1.0 to v2.0

### What Changed

**OLD (v1.0):**
```yaml
pipelines:
  - name_prefix: my_pipeline
    schedule:
      catalog.schema.table:  # Asset name as key
        cron: "0 0 2 * * ?"
```

**NEW (v2.0):**
```yaml
pipelines:
  - name_prefix: my_pipeline
    source_asset: catalog.schema.table  # Explicit field
    schedule:
      cron: "0 0 2 * * ?"
      timezone: "UTC"  # Required
```

### Migration Steps

1. **For each pipeline:**
   - Extract the asset name from the schedule key
   - Add `source_asset` field with that asset name
   - Flatten the schedule structure
   - Add `timezone` to the schedule

2. **Verify:**
   - Every pipeline has `source_asset`
   - Every cron schedule has `timezone`
   - All `source_asset` values match entries in `share_assets`

---

## 🧪 Testing Your SharePack

### 1. Upload and Validate

```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload_and_validate" \
  -H "X-Workspace-URL: https://adb-xxx.azuredatabricks.net" \
  -F "file=@my_sharepack.yaml"
```

**Expected Response:**
```json
{
  "Message": "Share pack uploaded successfully and queued for provisioning",
  "SharePackId": "abc-123-def-456",
  "SharePackName": "SharePack_test.user@jll.com_20240315_143022",
  "Status": "IN_PROGRESS",
  "ValidationErrors": [],
  "ValidationWarnings": []
}
```

### 2. Monitor Status

```bash
curl "http://localhost:8000/workflow/sharepack/abc-123-def-456"
```

**Provisioning Steps:**
- Step 1/7: Initializing provisioning
- Step 2/7: Creating recipients
- Step 3/7: Creating shares
- Step 4/7: Adding data objects to shares
- Step 5/7: Attaching recipients to shares
- Step 6/7: Creating DLT pipelines
- Step 7/7: Provisioning completed successfully

### 3. Check Results

**Success Response:**
```json
{
  "SharePackId": "abc-123-def-456",
  "Status": "COMPLETED",
  "ProvisioningStatus": "Provisioning completed successfully",
  "ErrorMessage": ""
}
```

**With Existing Resources (Idempotent):**
```
Logger output:
⚠️  Recipient external_partner_recipient already exists, skipping creation
✓ Created share: sales_analytics_share
⚠️  Pipeline sales_daily_sync already exists, skipping creation
✓ Created pipeline: sales_orders_sync
```

---

## 🎓 Examples in Sample Files

### sample_sharepack_v2.yaml

The sample YAML file demonstrates:

1. **Basic pipelines** with explicit source assets
2. **Pipeline-level overrides** (revenue_summary_sync uses custom catalog/schema)
3. **Multiple timezones** (America/New_York, UTC, Europe/London, Asia/Tokyo)
4. **Continuous schedules** (real-time streaming)
5. **Mixed SCD types** (Type 1 and Type 2)
6. **Multiple shares** (sales, operations, compliance)

### Excel Template CSVs

The CSV files include:
- 3 recipients (2 D2O, 1 D2D)
- 3 shares with different asset types
- 8 pipelines demonstrating all features
- Multiple notification emails
- Rich tagging examples

---

## 📞 Support

### API Documentation
- **Swagger UI:** `http://localhost:8000/`
- **Health Check:** `GET /workflow/health`
- **Upload:** `POST /workflow/sharepack/upload_and_validate`
- **Status:** `GET /workflow/sharepack/{share_pack_id}`

### Common Issues

**Issue: "source_asset not found in share_assets"**
- Solution: Ensure pipeline's `source_asset` exactly matches an entry in the share's `share_assets` list

**Issue: "Timezone required for cron schedule"**
- Solution: Add `timezone` field to all cron-based schedules

**Issue: "Invalid source table format"**
- Solution: Use 3-part names: `catalog.schema.table` (not just table name)

**Issue: "Pipeline already exists"**
- Solution: This is expected with idempotency - system skips and continues

---

## 🎯 Best Practices

1. **One pipeline per asset:** Each asset in `share_assets` should have exactly one pipeline
2. **Explicit naming:** Use descriptive `name_prefix` values (e.g., `sales_daily_sync` not `pipeline1`)
3. **Timezone consistency:** Group related pipelines by timezone for easier management
4. **Override sparingly:** Only use pipeline-level catalog/schema overrides when necessary
5. **Tag everything:** Use tags for environment, owner, dataset, compliance, etc.
6. **Test in dev first:** Upload to dev workspace before production
7. **Version your SharePacks:** Include version in metadata for tracking

---

## 🚨 Important Notes

- **Backwards incompatible:** v2.0 structure is NOT compatible with v1.0
- **Migrate existing SharePacks:** Use the migration guide above
- **Idempotency:** Re-uploading the same SharePack is safe - existing resources are skipped
- **Queue processing:** Background queue polls every 5 seconds (10-minute visibility timeout)
- **Resource naming:** All names (recipient, share, pipeline) must be unique

---

## 📚 Additional Resources

- **Full Excel Template Docs:** `sample_sharepack_template_v2.md`
- **CSV Files:** `excel_template/` directory
- **Python Generator:** `excel_template/create_excel_template.py`
- **Project Instructions:** `.claude/CLAUDE.md`

---

**Version:** 2.0
**Last Updated:** 2024-03-15
**Breaking Changes:** Yes (from v1.0)
