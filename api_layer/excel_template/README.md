# SharePack Excel Template v2.0

This directory contains CSV files for creating a SharePack Excel template.

## Creating the Excel Workbook

### Option 1: Manual Import (Recommended)
1. Create a new Excel workbook
2. Create 4 sheets named: `metadata`, `recipient`, `share`, `pipelines`
3. Import each CSV file into its corresponding sheet:
   - Import `01_metadata.csv` → `metadata` sheet
   - Import `02_recipient.csv` → `recipient` sheet
   - Import `03_share.csv` → `share` sheet
   - Import `04_pipelines.csv` → `pipelines` sheet
4. Save as `sample_sharepack_v2.xlsx`

### Option 2: Using Python (pandas)
```python
import pandas as pd

# Read CSV files
metadata = pd.read_csv('01_metadata.csv')
recipient = pd.read_csv('02_recipient.csv')
share = pd.read_csv('03_share.csv')
pipelines = pd.read_csv('04_pipelines.csv')

# Create Excel writer
with pd.ExcelWriter('sample_sharepack_v2.xlsx', engine='openpyxl') as writer:
    metadata.to_excel(writer, sheet_name='metadata', index=False)
    recipient.to_excel(writer, sheet_name='recipient', index=False)
    share.to_excel(writer, sheet_name='share', index=False)
    pipelines.to_excel(writer, sheet_name='pipelines', index=False)

print("✓ Created sample_sharepack_v2.xlsx")
```

## Key Features in v2.0

### 1. **Explicit source_asset Column**
Every pipeline now has a `source_asset` column that explicitly states which share asset it processes.

**Before (v1):**
```
Pipeline extracted asset from schedule key (error-prone)
```

**Now (v2):**
```csv
name_prefix,source_asset
sales_daily_sync,main_catalog.sales_schema.daily_sales
```

### 2. **Pipeline-Level Catalog/Schema Overrides**
Pipelines can override the default target catalog/schema:

```csv
share_name,name_prefix,source_asset,ext_catalog_name,ext_schema_name
sales_analytics_share,revenue_summary_sync,main_catalog.sales_schema.revenue_summary,finance_catalog,revenue_reports
```

This pipeline writes to `finance_catalog.revenue_reports` instead of the share's default.

### 3. **Timezone Support**
All cron-based schedules require a timezone:

```csv
schedule_type,cron_expression,timezone
cron,0 0 2 * * ?,America/New_York
cron,0 0 1 * * ?,Europe/London
continuous,,
```

### 4. **Simplified Schedule Structure**
- `schedule_type`: `cron` or `continuous`
- `cron_expression`: Cron string (6-part format)
- `timezone`: Standard timezone string

## Cell Separator

Multi-value cells use pipe `|` separator:
- `recipients`: `recipient1|recipient2`
- `share_assets`: `catalog.schema.table1|catalog.schema.table2`
- `key_columns`: `col1|col2|col3`
- `tags`: `key1=value1|key2=value2`
- `notification`: `email1@jll.com|email2@jll.com`

## Validation Rules

1. **One pipeline per share_asset**: Each asset in `share_assets` must have a corresponding pipeline
2. **source_asset must match**: Pipeline's `source_asset` must exist in share's `share_assets`
3. **3-part names**: All assets must be `catalog.schema.table`
4. **Cron requires timezone**: If `schedule_type=cron`, `timezone` is required
5. **D2D requires metastore ID**: If `type=D2D`, `recipient_databricks_org` is required

## Example Data

The CSV files include example data for:
- 3 recipients (2 D2O, 1 D2D)
- 3 shares (sales, operations, compliance)
- 8 pipelines demonstrating:
  - Explicit source assets
  - Pipeline-level catalog/schema overrides
  - Multiple timezones (America/New_York, UTC, Europe/London, Asia/Tokyo)
  - Continuous and cron schedules
  - SCD Type 1 and Type 2

## Testing

After creating your Excel file:

1. Upload via API:
```bash
curl -X POST "http://localhost:8000/workflow/sharepack/upload_and_validate" \
  -H "X-Workspace-URL: https://your-workspace.azuredatabricks.net" \
  -F "file=@sample_sharepack_v2.xlsx"
```

2. Monitor status:
```bash
curl "http://localhost:8000/workflow/sharepack/{share_pack_id}"
```

## Full Documentation

See `sample_sharepack_template_v2.md` for complete field descriptions and examples.
