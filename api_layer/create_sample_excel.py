#!/usr/bin/env python3
"""
Generate sample Excel sharepack file for workflow testing.

Usage:
    python create_sample_excel.py

Output:
    sample_sharepack.xlsx
"""

from pathlib import Path

import openpyxl
from openpyxl.styles import Font
from openpyxl.styles import PatternFill


def create_sample_excel():
    """Create sample Excel sharepack file."""

    # Create workbook
    wb = openpyxl.Workbook()

    # Remove default sheet
    wb.remove(wb.active)

    # =========================================================================
    # Sheet 1: Metadata
    # =========================================================================
    ws_metadata = wb.create_sheet("Metadata", 0)

    # Header styling
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    # Headers
    ws_metadata["A1"] = "Field"
    ws_metadata["B1"] = "Value"
    ws_metadata["A1"].fill = header_fill
    ws_metadata["B1"].fill = header_fill
    ws_metadata["A1"].font = header_font
    ws_metadata["B1"].font = header_font

    # Data
    metadata_data = [
        ("requestor", "test.user@jll.com"),
        ("project_name", "Test Project - Q1 2024"),
        ("business_line", "Test Business Line"),
        ("strategy", "NEW"),
        ("description", "Test share pack for workflow validation"),
    ]

    for idx, (field, value) in enumerate(metadata_data, start=2):
        ws_metadata[f"A{idx}"] = field
        ws_metadata[f"B{idx}"] = value

    # Column widths
    ws_metadata.column_dimensions["A"].width = 20
    ws_metadata.column_dimensions["B"].width = 50

    # =========================================================================
    # Sheet 2: Recipients
    # =========================================================================
    ws_recipients = wb.create_sheet("Recipients", 1)

    # Headers
    recipient_headers = ["name", "type", "email", "metastore_id", "allowed_ips", "comment"]
    for idx, header in enumerate(recipient_headers, start=1):
        cell = ws_recipients.cell(row=1, column=idx)
        cell.value = header
        cell.fill = header_fill
        cell.font = header_font

    # Data
    recipient_data = [
        (
            "test-recipient-d2o",
            "D2O",
            "test@example.com",
            "",
            "192.168.1.0/24,10.0.0.50",
            "Test D2O recipient for validation",
        ),
        ("test-recipient-d2d", "D2D", "", "aws:us-west-2:abc-123-def-456", "", "Test D2D recipient for validation"),
    ]

    for row_idx, row_data in enumerate(recipient_data, start=2):
        for col_idx, value in enumerate(row_data, start=1):
            ws_recipients.cell(row=row_idx, column=col_idx).value = value

    # Column widths
    ws_recipients.column_dimensions["A"].width = 20  # name
    ws_recipients.column_dimensions["B"].width = 8  # type
    ws_recipients.column_dimensions["C"].width = 25  # email
    ws_recipients.column_dimensions["D"].width = 30  # metastore_id
    ws_recipients.column_dimensions["E"].width = 30  # allowed_ips
    ws_recipients.column_dimensions["F"].width = 40  # comment

    # =========================================================================
    # Sheet 3: Shares
    # =========================================================================
    ws_shares = wb.create_sheet("Shares", 2)

    # Headers
    share_headers = ["name", "comment", "recipients", "data_objects"]
    for idx, header in enumerate(share_headers, start=1):
        cell = ws_shares.cell(row=1, column=idx)
        cell.value = header
        cell.fill = header_fill
        cell.font = header_font

    # Data
    share_data = [
        (
            "test_share_q1",
            "Q1 test data share",
            "test-recipient-d2o,test-recipient-d2d",
            "catalog.schema.test_table_1,catalog.schema.test_table_2",
        ),
        (
            "test_audit_share",
            "Test audit logs",
            "test-recipient-d2o",
            "catalog.audit_schema.activity_log",
        ),
    ]

    for row_idx, row_data in enumerate(share_data, start=2):
        for col_idx, value in enumerate(row_data, start=1):
            ws_shares.cell(row=row_idx, column=col_idx).value = value

    # Column widths
    ws_shares.column_dimensions["A"].width = 20  # name
    ws_shares.column_dimensions["B"].width = 25  # comment
    ws_shares.column_dimensions["C"].width = 50  # recipients
    ws_shares.column_dimensions["D"].width = 60  # data_objects

    # =========================================================================
    # Sheet 4: Pipelines (empty for MVP)
    # =========================================================================
    ws_pipelines = wb.create_sheet("Pipelines", 3)

    # Headers
    pipeline_headers = ["name", "type", "schedule", "source", "destination", "comment"]
    for idx, header in enumerate(pipeline_headers, start=1):
        cell = ws_pipelines.cell(row=1, column=idx)
        cell.value = header
        cell.fill = header_fill
        cell.font = header_font

    # Column widths
    ws_pipelines.column_dimensions["A"].width = 20
    ws_pipelines.column_dimensions["B"].width = 15
    ws_pipelines.column_dimensions["C"].width = 20
    ws_pipelines.column_dimensions["D"].width = 40
    ws_pipelines.column_dimensions["E"].width = 40
    ws_pipelines.column_dimensions["F"].width = 40

    # =========================================================================
    # Save workbook
    # =========================================================================
    output_path = Path(__file__).parent / "sample_sharepack.xlsx"
    wb.save(output_path)

    print(f"✓ Created sample Excel file: {output_path}")
    print(f"✓ Sheets: {', '.join(wb.sheetnames)}")
    print(f"✓ Recipients: {len(recipient_data)}")
    print(f"✓ Shares: {len(share_data)}")
    print()
    print("To test:")
    print(f"  curl -X POST http://localhost:8000/workflow/sharepack/upload \\")
    print(f"    -H 'X-Workspace-URL: https://adb-xxx.azuredatabricks.net' \\")
    print(f"    -F 'file=@{output_path.name}'")


if __name__ == "__main__":
    create_sample_excel()
