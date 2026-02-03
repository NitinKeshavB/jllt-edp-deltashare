#!/usr/bin/env python3
"""
SharePack Excel Template Generator v2.0

Creates sample_sharepack_v2.xlsx from CSV files.
"""

from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("❌ pandas not installed. Install with: pip install pandas openpyxl")
    exit(1)


def create_excel_template():
    """Create Excel workbook from CSV files."""

    script_dir = Path(__file__).parent
    output_file = script_dir.parent / "sample_sharepack_v2.xlsx"

    csv_files = {
        "metadata": "01_metadata.csv",
        "recipient": "02_recipient.csv",
        "share": "03_share.csv",
        "pipelines": "04_pipelines.csv",
    }

    print("📊 Creating SharePack Excel Template v2.0...")
    print()

    # Read CSV files
    dataframes = {}
    for sheet_name, csv_file in csv_files.items():
        csv_path = script_dir / csv_file
        if not csv_path.exists():
            print(f"❌ File not found: {csv_file}")
            return False

        df = pd.read_csv(csv_path, sep=",")
        dataframes[sheet_name] = df
        print(f"✓ Loaded {csv_file}: {len(df)} rows, {len(df.columns)} columns")

    print()
    print(f"📝 Writing to: {output_file}")

    # Create Excel writer
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets[sheet_name]
            for idx, col in enumerate(df.columns):
                max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)

    print()
    print("✅ Success! Created sample_sharepack_v2.xlsx")
    print()
    print("📋 Workbook contains 4 sheets:")
    for sheet_name, df in dataframes.items():
        print(f"   - {sheet_name}: {len(df)} rows")

    print()
    print("🔍 Key Features:")
    print("   ✓ Explicit source_asset field in pipelines")
    print("   ✓ Pipeline-level catalog/schema overrides")
    print("   ✓ Timezone support for all cron schedules")
    print("   ✓ Multiple examples (sales, operations, compliance)")
    print()
    print("📤 Upload to API:")
    print(f"   POST /workflow/sharepack/upload_and_validate")
    print(f"   Header: X-Workspace-URL: https://your-workspace.azuredatabricks.net")
    print(f"   File: {output_file.name}")
    print()

    return True


if __name__ == "__main__":
    success = create_excel_template()
    exit(0 if success else 1)
