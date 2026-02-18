# Share Pack Templates

Full reference templates for share pack configuration (YAML and Excel). Both formats use the **same validation** at upload: `validate_metadata` and `validate_sharepack_config` from the orchestrator.

## Files

| File | Description |
|------|-------------|
| `sample_sharepack.yaml` | Full YAML template (NEW/UPDATE strategy). Use for manual edit or CI. |
| `sample_sharepack.xlsx` | Full Excel template. **Regenerate** with the script below. |

## Regenerating the Excel template

From the `api_layer` directory (with project deps installed, e.g. `openpyxl`):

```bash
python scripts/create_sample_excel.py
```

This writes `sharepack_templates/sample_sharepack.xlsx` with sheets: **Metadata**, **Recipients**, **Shares**, **Pipelines**. Column names and sample data match the parser and the YAML template.

## Validation (YAML and Excel)

- **Parser**: Both YAML and Excel are parsed into `SharePackConfig` (Pydantic).
- **Strict validation**: After parsing, the same checks as provisioning run:
  - `validate_metadata(config["metadata"])` — workspace_url, servicenow, approver_status, etc.
  - `validate_sharepack_config(config)` — recipients, shares, pipelines, share_assets vs `source_asset`, schedule for non-continuous pipelines.

Upload and provisioning use this validation; invalid files return 400 with a clear error.

## Validators (optional)

- `validate_yaml.py` — validate a YAML file.
- `validate_excel.py` — validate an Excel file (structure and content).

Usage: `python validate_excel.py sample_sharepack.xlsx`
