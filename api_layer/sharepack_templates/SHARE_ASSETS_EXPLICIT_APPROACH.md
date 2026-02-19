# Share Assets - Explicit Approach

## Overview

Share assets can now be managed using the **explicit approach** with `share_assets_to_add` and `share_assets_to_remove` fields. This is safer than the declarative approach as it only specifies incremental changes rather than replacing the entire state.

## YAML Syntax

```yaml
shares:
  - name: my_share
    description: "Example share"

    # Explicit approach (RECOMMENDED)
    share_assets_to_add:
      - catalog.schema.new_table
      - catalog.schema.another_table

    share_assets_to_remove:
      - catalog.schema.old_table

    # Recipients can also use explicit approach
    recipients_to_add:
      - new_recipient

    recipients_to_remove:
      - old_recipient
```

## Use Cases

### 1. Add new assets to an existing share
```yaml
shares:
  - name: sales_data_share
    share_assets_to_add:
      - main_catalog.sales.transactions_2024
      - main_catalog.sales.customers_v2
```

### 2. Remove assets from an existing share
```yaml
shares:
  - name: sales_data_share
    share_assets_to_remove:
      - main_catalog.sales.deprecated_table
      - main_catalog.sales.old_view
```

### 3. Add and remove assets simultaneously
```yaml
shares:
  - name: analytics_share
    share_assets_to_add:
      - warehouse.analytics.new_metrics
      - warehouse.analytics.daily_reports

    share_assets_to_remove:
      - warehouse.analytics.legacy_metrics
```

### 4. Create new share with initial assets
```yaml
shares:
  - name: new_share
    description: "Newly created share"
    share_assets_to_add:
      - catalog.schema.table1
      - catalog.schema.table2

    recipients_to_add:
      - recipient_a
      - recipient_b
```

## Validation Rules

The ShareConfig model enforces the following validations:

1. **No overlap between add and remove**: An asset cannot be in both `share_assets_to_add` and `share_assets_to_remove`
   ```yaml
   # ❌ INVALID - asset1 is in both lists
   share_assets_to_add:
     - catalog.schema.asset1
   share_assets_to_remove:
     - catalog.schema.asset1
   ```

2. **Cannot mix declarative and explicit for remove**: Cannot use `share_assets` (declarative) together with `share_assets_to_remove` (explicit)
   ```yaml
   # ❌ INVALID - mixing approaches
   share_assets:
     - catalog.schema.table1
   share_assets_to_remove:
     - catalog.schema.table2
   ```

3. **Can mix declarative with add**: You can use `share_assets` (declarative) with `share_assets_to_add` (explicit)
   ```yaml
   # ✅ VALID - declarative list + additions
   share_assets:
     - catalog.schema.existing_table
   share_assets_to_add:
     - catalog.schema.new_table
   ```

## Behavior

### For New Shares
- `share_assets_to_add`: Assets are added to the newly created share
- `share_assets_to_remove`: No effect (share doesn't exist yet)

### For Existing Shares
The orchestrator computes the desired state as:
```python
desired_assets = (current_assets | assets_to_add) - assets_to_remove
```

Example:
- Current assets in share: `[table1, table2, table3]`
- Config: `share_assets_to_add: [table4]`, `share_assets_to_remove: [table2]`
- Result: Share will have `[table1, table3, table4]`

## Comparison with Declarative Approach

### Declarative Approach (share_assets)
```yaml
# Replaces ALL assets with this exact list
share_assets:
  - catalog.schema.table1
  - catalog.schema.table2
```
- **Risk**: If you forget an existing asset, it gets removed
- **Benefit**: Ensures exact state

### Explicit Approach (share_assets_to_add / share_assets_to_remove)
```yaml
# Only specifies what to add/remove
share_assets_to_add:
  - catalog.schema.new_table

share_assets_to_remove:
  - catalog.schema.old_table
```
- **Benefit**: Safer - only modifies what you specify
- **Benefit**: Clear intent (adding vs removing)
- **Recommendation**: **Use this approach for production safety**

## Complete Example

```yaml
sharepack:
  name: "quarterly_update_2024_q1"
  description: "Q1 2024 data updates"
  configurator: "data.team@company.com"
  strategy: "UPDATE"  # Modify existing shares

shares:
  - name: customer_analytics_share
    description: "Customer analytics data"

    # Add new Q1 2024 tables
    share_assets_to_add:
      - analytics.customers.transactions_2024_q1
      - analytics.customers.behavior_2024_q1

    # Remove old 2023 Q4 tables
    share_assets_to_remove:
      - analytics.customers.transactions_2023_q4
      - analytics.customers.behavior_2023_q4

    # Add new recipient for Q1
    recipients_to_add:
      - q1_analytics_team

    delta_share:
      ext_catalog_name: customer_analytics
      ext_schema_name: quarterly_data
      tags:
        - analytics
        - customer_data
        - q1_2024

  - name: sales_reporting_share
    description: "Sales reporting data"

    # Only add new assets, keep all existing
    share_assets_to_add:
      - sales.reports.monthly_summary_2024_01
      - sales.reports.monthly_summary_2024_02
      - sales.reports.monthly_summary_2024_03

    recipients_to_add:
      - executive_dashboard
```

## Asset Format

Assets can be:
- **Tables/Views**: Full three-part names like `catalog.schema.table_name`
- **Schemas**: Two-part names like `catalog.schema_name`

Example:
```yaml
share_assets_to_add:
  - main_catalog.sales.transactions      # Table
  - main_catalog.analytics.revenue_view  # View
  - main_catalog.reporting               # Entire schema
```

## Logging

When using explicit approach, the orchestrator logs:
```
Share my_share: Using explicit share_assets.
Current={'table1', 'table2'},
Add={'table3'},
Remove={'table1'},
Desired=['table2', 'table3']
```

This helps verify the computed desired state before changes are applied.
