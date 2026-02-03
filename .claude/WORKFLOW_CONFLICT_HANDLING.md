# Workflow Conflict Handling Guide

## Problem: Using NEW Strategy with Existing Resources

When a user uploads a share pack with `strategy: NEW` but resources (recipients, shares, pipelines) already exist in Databricks, conflicts occur.

---

## Current MVP Behavior

### What Happens Now

The MVP orchestrator is currently a **stub implementation** that logs actions without actually calling Databricks SDK. When you implement the real provisioning logic, you'll encounter these conflicts.

**From [provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py):**
```python
# Current MVP stub (lines 56-73)
for recipient_config in config["recipient"]:
    recipient_name = recipient_config["name"]
    logger.debug(f"Would create recipient: {recipient_name}")

    # TODO: For production implementation:
    # result = create_recipient(workspace_url, recipient_config, ...)
    # if isinstance(result, str):
    #     raise Exception(f"Failed to create recipient: {result}")
```

When you replace these stubs with real SDK calls, you'll see errors like:
```python
databricks.sdk.errors.ResourceAlreadyExists: Recipient 'finance-auditor' already exists
```

---

## Solution Options

### Option 1: Pre-Flight Validation (Recommended)

Check if resources exist BEFORE attempting to create them.

#### Implementation

Create a new validator: `workflow/validators/databricks_conflict_validator.py`

```python
"""
Databricks Conflict Validator

Checks if resources already exist in Databricks before attempting creation.
Used with NEW strategy to prevent conflicts.
"""

from typing import Dict, List, Tuple
from databricks.sdk import WorkspaceClient
from loguru import logger


class ConflictValidationResult:
    """Result of conflict validation check."""

    def __init__(self):
        self.is_valid = True
        self.conflicts: List[str] = []

    def add_conflict(self, resource_type: str, resource_name: str):
        """Add a conflict to the result."""
        self.is_valid = False
        conflict_msg = f"{resource_type} '{resource_name}' already exists"
        self.conflicts.append(conflict_msg)
        logger.warning(conflict_msg)


async def validate_no_conflicts(
    workspace_url: str,
    config: Dict,
    strategy: str
) -> ConflictValidationResult:
    """
    Validate that resources don't already exist when using NEW strategy.

    Args:
        workspace_url: Databricks workspace URL
        config: Parsed share pack configuration
        strategy: Provisioning strategy (NEW or UPDATE)

    Returns:
        ConflictValidationResult with conflicts list
    """
    result = ConflictValidationResult()

    # Only validate for NEW strategy
    if strategy != "NEW":
        logger.debug("Skipping conflict validation for UPDATE strategy")
        return result

    # Get Databricks client
    from dbrx_api.dbrx_auth.token_gen import get_auth_token
    from datetime import datetime, timezone

    session_token = get_auth_token(datetime.now(timezone.utc))[0]
    w_client = WorkspaceClient(host=workspace_url, token=session_token)

    try:
        # Check recipients
        existing_recipients = {r.name for r in w_client.recipients.list()}
        for recipient_config in config.get("recipient", []):
            recipient_name = recipient_config["name"]
            if recipient_name in existing_recipients:
                result.add_conflict("Recipient", recipient_name)

        # Check shares
        existing_shares = {s.name for s in w_client.shares.list_shares()}
        for share_config in config.get("share", []):
            share_name = share_config["name"]
            if share_name in existing_shares:
                result.add_conflict("Share", share_name)

        # Note: Pipelines and schedules don't have conflicts since they're project-specific

    except Exception as e:
        logger.error(f"Error during conflict validation: {e}", exc_info=True)
        result.is_valid = False
        result.conflicts.append(f"Validation error: {str(e)}")

    return result
```

#### Integration in Routes

Update [routes_workflow.py](src/dbrx_api/routes/routes_workflow.py) upload endpoint:

```python
@ROUTER_WORKFLOW.post("/sharepack/upload")
async def upload_sharepack(...):
    # ... existing parsing code ...

    # Validate no conflicts for NEW strategy
    if config.metadata.strategy == "NEW":
        from dbrx_api.workflow.validators.databricks_conflict_validator import (
            validate_no_conflicts
        )

        validation_result = await validate_no_conflicts(
            workspace_url=workspace_url,
            config=config.dict(),
            strategy=config.metadata.strategy
        )

        if not validation_result.is_valid:
            logger.warning(f"Conflict validation failed: {validation_result.conflicts}")
            return SharePackUploadResponse(
                Message="Share pack validation failed due to conflicts",
                SharePackId=str(share_pack_id),
                SharePackName=share_pack_name,
                Status="VALIDATION_FAILED",
                ValidationErrors=validation_result.conflicts,
                ValidationWarnings=[]
            )

    # ... continue with storage and enqueue ...
```

#### User Experience

**With conflicts:**
```json
{
  "Message": "Share pack validation failed due to conflicts",
  "SharePackId": "550e8400-e29b-41d4-a716-446655440000",
  "SharePackName": "SharePack_john.doe_20240130_143022",
  "Status": "VALIDATION_FAILED",
  "ValidationErrors": [
    "Recipient 'finance-auditor' already exists",
    "Share 'finance_q1_share' already exists"
  ],
  "ValidationWarnings": []
}
```

**User action:** Change to `strategy: UPDATE` or rename resources.

---

### Option 2: Idempotent Creation (Try-Create Pattern)

Attempt to create, catch conflict errors, and continue if resource exists.

#### Implementation

Update [provisioning.py](src/dbrx_api/workflow/orchestrator/provisioning.py):

```python
async def provision_sharepack_new(pool, share_pack: Dict) -> None:
    """
    Provision share pack with NEW strategy (idempotent).

    Uses try-create pattern: attempts to create resources,
    silently continues if they already exist.
    """
    # ... existing setup code ...

    # Step 4: Create Recipients (idempotent)
    await tracker.update("Step 4/8: Creating recipients")
    for recipient_config in config["recipient"]:
        recipient_name = recipient_config["name"]

        try:
            # Attempt to create recipient
            result = create_recipient_d2o(workspace_url, recipient_config, ...)
            if isinstance(result, str):
                raise Exception(result)

            logger.info(f"Created recipient: {recipient_name}")

        except ResourceAlreadyExistsError:
            # Resource exists - this is OK for idempotent operation
            logger.info(f"Recipient {recipient_name} already exists - skipping")
            continue

        except Exception as e:
            # Other errors are failures
            raise Exception(f"Failed to create recipient {recipient_name}: {e}")

    # Step 5: Create Shares (idempotent)
    await tracker.update("Step 5/8: Creating shares")
    for share_config in config["share"]:
        share_name = share_config["name"]

        try:
            result = create_share(workspace_url, share_name, ...)
            if isinstance(result, str):
                raise Exception(result)

            logger.info(f"Created share: {share_name}")

        except ResourceAlreadyExistsError:
            logger.info(f"Share {share_name} already exists - skipping")
            continue

        except Exception as e:
            raise Exception(f"Failed to create share {share_name}: {e}")
```

#### Pros & Cons

**Pros:**
- ✅ True idempotency - can run multiple times safely
- ✅ Simpler user experience - no pre-validation needed
- ✅ Handles partial failures gracefully

**Cons:**
- ❌ Might mask actual errors
- ❌ Existing resources might not match expected configuration
- ❌ No warning to user about conflicts

---

### Option 3: Smart Strategy Detection (Auto-Detect)

Automatically detect if resources exist and switch strategy.

#### Implementation

```python
async def detect_strategy(workspace_url: str, config: Dict) -> str:
    """
    Auto-detect appropriate strategy based on existing resources.

    Returns:
        "NEW" if no resources exist
        "UPDATE" if any resources exist
    """
    w_client = WorkspaceClient(host=workspace_url, token=session_token)

    # Check if any recipients exist
    existing_recipients = {r.name for r in w_client.recipients.list()}
    recipient_names = {r["name"] for r in config["recipient"]}

    if existing_recipients & recipient_names:
        # Intersection exists - some recipients already created
        return "UPDATE"

    # Check if any shares exist
    existing_shares = {s.name for s in w_client.shares.list_shares()}
    share_names = {s["name"] for s in config["share"]}

    if existing_shares & share_names:
        return "UPDATE"

    return "NEW"
```

#### Integration

```python
@ROUTER_WORKFLOW.post("/sharepack/upload")
async def upload_sharepack(...):
    # ... parsing ...

    # Auto-detect strategy if needed
    user_strategy = config.metadata.strategy
    detected_strategy = await detect_strategy(workspace_url, config.dict())

    if user_strategy == "NEW" and detected_strategy == "UPDATE":
        logger.warning(
            f"User specified NEW but resources exist. "
            f"Auto-switching to UPDATE strategy."
        )
        config.metadata.strategy = "UPDATE"

        return SharePackUploadResponse(
            Message="Share pack uploaded. Strategy auto-changed from NEW to UPDATE.",
            SharePackId=str(share_pack_id),
            SharePackName=share_pack_name,
            Status="IN_PROGRESS",
            ValidationErrors=[],
            ValidationWarnings=[
                f"Strategy changed from NEW to UPDATE because resources exist"
            ]
        )
```

---

## Recommended Approach

**For Production, use a combination:**

1. **Pre-flight validation** (Option 1) - Fail fast with clear errors
2. **Smart naming conventions** - Prevent conflicts proactively
3. **User guidance** - Help users choose correct strategy

### Phased Implementation

#### Phase 1: Enhanced Error Messages (Quick Win)

Improve error messages to guide users:

```python
except ResourceAlreadyExistsError as e:
    error_msg = (
        f"Resource already exists: {str(e)}. "
        f"If you want to modify existing resources, use 'strategy: UPDATE'. "
        f"If you want to create new resources, rename them in your config file."
    )
    await tracker.fail(error_msg)
    raise Exception(error_msg)
```

#### Phase 2: Pre-Flight Validation (Recommended)

Implement Option 1 - validate before enqueuing.

#### Phase 3: Conflict Resolution UI (Future)

Provide UI/API for conflict resolution:
```json
{
  "Conflicts": [
    {
      "Type": "Recipient",
      "Name": "finance-auditor",
      "Action": "rename|skip|overwrite"
    }
  ]
}
```

---

## Best Practices for Users

### 1. Naming Convention

Use unique, descriptive names:

```yaml
recipient:
  - name: {business_line}_{project}_{recipient_type}_{date}
    # Example: finance_q1audit_external_20240130
```

### 2. Pre-Check Before Upload

Query Databricks before creating share pack:

```bash
# List existing recipients
curl -X GET "https://workspace.azuredatabricks.net/api/2.1/unity-catalog/recipients" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"

# List existing shares
curl -X GET "https://workspace.azuredatabricks.net/api/2.1/unity-catalog/shares" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"
```

### 3. Use UPDATE for Modifications

First upload: `strategy: NEW`
All subsequent uploads: `strategy: UPDATE`

### 4. Version Your Share Packs

Keep history of what's been created:

```
sharepacks/
  finance_q1/
    v1_NEW_20240130.yaml       # Initial creation
    v2_UPDATE_20240206.yaml    # Added recipient
    v3_UPDATE_20240213.yaml    # Added tables
```

---

## Testing Conflict Scenarios

### Test 1: Duplicate Recipient

```yaml
# First upload (succeeds)
metadata:
  strategy: NEW
recipient:
  - name: test-recipient-001
    type: D2O
    email: test@example.com

# Second upload (should fail or warn)
metadata:
  strategy: NEW  # ← Wrong strategy!
recipient:
  - name: test-recipient-001  # ← Already exists
    type: D2O
    email: test@example.com
```

**Expected behavior:**
- With validation: Immediate 400 error with conflict list
- Without validation: Provisioning fails with ResourceAlreadyExists

### Test 2: Duplicate Share

```yaml
# First upload
share:
  - name: test_share_001
    recipients: [test-recipient-001]

# Second upload (should fail)
share:
  - name: test_share_001  # ← Already exists
    recipients: [test-recipient-002]
```

### Test 3: Mixed (Some Exist, Some Don't)

```yaml
metadata:
  strategy: NEW
recipient:
  - name: existing-recipient-001  # ← Already exists
  - name: new-recipient-002        # ← Doesn't exist
```

**Expected behavior:**
- Pre-validation: Fails with conflict on existing-recipient-001
- Idempotent: Skips existing-recipient-001, creates new-recipient-002

---

## Implementation Priority

1. **Immediate (This Week):** Enhanced error messages
2. **Short-term (Next Sprint):** Pre-flight validation (Option 1)
3. **Medium-term (Next Quarter):** Idempotent operations (Option 2)
4. **Long-term (Future):** Smart auto-detection (Option 3)

---

## Code Changes Needed

### File: `workflow/validators/databricks_conflict_validator.py` (NEW)

Create the validator as shown in Option 1.

### File: `routes/routes_workflow.py` (MODIFY)

Add validation call in upload endpoint (lines 34-62).

### File: `orchestrator/provisioning.py` (MODIFY)

When implementing real Databricks SDK calls, add try-catch for ResourceAlreadyExistsError.

### File: `errors.py` (MODIFY)

Add specific error class:

```python
class ResourceConflictError(Exception):
    """Raised when resource already exists and NEW strategy used."""

    def __init__(self, resource_type: str, resource_name: str):
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(f"{resource_type} '{resource_name}' already exists")
```

---

## Summary

**Current MVP:** Will fail with generic error if resources exist

**Recommendation:** Implement Option 1 (Pre-flight validation) when you connect real Databricks SDK

**User Guidance:**
- Use unique naming conventions
- Check Databricks before uploading with NEW
- Use UPDATE strategy for modifications
- Keep share pack version history

**Implementation:** Add `databricks_conflict_validator.py` and integrate into upload endpoint

Would you like me to implement Option 1 (pre-flight validation) now, or wait until you're ready to integrate the real Databricks SDK calls?
