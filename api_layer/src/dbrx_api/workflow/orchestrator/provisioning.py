"""
Share Pack Provisioning - FULL Implementation

Implements complete provisioning with actual Databricks API calls.
Tracks all resources in database with rollback support.
"""

from typing import Any
from typing import Dict
from urllib.parse import urlparse

import requests
from loguru import logger

from dbrx_api.workflow.db.repository_pipeline import PipelineRepository
from dbrx_api.workflow.db.repository_recipient import RecipientRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from dbrx_api.workflow.orchestrator.db_persist import persist_pipelines_to_db
from dbrx_api.workflow.orchestrator.db_persist import persist_recipients_to_db
from dbrx_api.workflow.orchestrator.db_persist import persist_shares_to_db
from dbrx_api.workflow.orchestrator.pipeline_flow import _rollback_pipelines
from dbrx_api.workflow.orchestrator.pipeline_flow import ensure_pipelines
from dbrx_api.workflow.orchestrator.recipient_flow import _rollback_recipients
from dbrx_api.workflow.orchestrator.recipient_flow import ensure_recipients
from dbrx_api.workflow.orchestrator.share_flow import _rollback_shares
from dbrx_api.workflow.orchestrator.share_flow import ensure_shares
from dbrx_api.workflow.orchestrator.status_tracker import StatusTracker


def validate_sharepack_config(config: Dict[str, Any]) -> None:
    """
    Validate sharepack configuration consistency for NEW/UPDATE strategies.

    You can pass recipients alone, shares alone, or schedules alone (shares with pipelines).
    At least one of recipient or share must be non-empty.

    Ensures:
    1. Required: metadata; recipient and share are lists; at least one of them non-empty
    2. Recipients: each has non-empty name; type D2D or D2O; D2D has recipient_databricks_org (no recipient_ips); D2O no recipient_databricks_org; contact email valid if set; token_expiry >= 0; no duplicate names
    3. Shares: each has non-empty name; if pipelines present, delta_share with ext_catalog_name and ext_schema_name; recipients a list
    4. When share_assets are specified: every asset must have a pipeline with that source_asset (fail with request for pipeline details if missing)
    5. Every non-continuous pipeline must have schedule info (cron/timezone or continuous)
    6. Share asset strings are non-empty qualified names (no empty segments)

    Args:
        config: Full sharepack configuration dictionary

    Raises:
        ValueError: If validation fails
    """
    logger.info("=" * 80)
    logger.info("SHAREPACK CONFIGURATION VALIDATION")
    logger.info("=" * 80)

    if not isinstance(config.get("metadata"), dict):
        raise ValueError("Share pack config must contain 'metadata' section")

    recipients = config.get("recipient")
    if not isinstance(recipients, list):
        raise ValueError("Share pack config must contain 'recipient' section (list)")

    shares = config.get("share")
    if not isinstance(shares, list):
        raise ValueError("Share pack config must contain 'share' section (list)")

    if not recipients and not shares:
        raise ValueError(
            "Share pack must contain at least one of: 'recipient' or 'share'. "
            "You can pass recipients alone, shares alone, or schedules alone (shares with pipelines)."
        )

    seen_recipient_names = set()
    for idx, recip in enumerate(recipients):
        if not isinstance(recip, dict):
            raise ValueError(f"Recipient at index {idx} must be a dictionary")
        name = recip.get("name")
        if not name or not str(name).strip():
            raise ValueError(f"Recipient at index {idx} must have a non-empty 'name' field")
        name = str(name).strip()
        if name in seen_recipient_names:
            raise ValueError(f"Duplicate recipient name: '{name}'")
        seen_recipient_names.add(name)

        recip_type = recip.get("type")
        if not recip_type or str(recip_type).strip().upper() not in ("D2D", "D2O"):
            raise ValueError(f"Recipient '{name}': 'type' must be D2D or D2O (got: {recip_type!r})")
        recip_type = str(recip_type).strip().upper()

        if recip_type == "D2D":
            org = recip.get("recipient_databricks_org")
            if not org or not str(org).strip():
                raise ValueError(f"Recipient '{name}' (D2D): 'recipient_databricks_org' is required")
            if recip.get("recipient_ips"):
                raise ValueError(f"Recipient '{name}' (D2D): cannot have 'recipient_ips' (D2O only)")
        else:
            if recip.get("recipient_databricks_org"):
                raise ValueError(f"Recipient '{name}' (D2O): must not set 'recipient_databricks_org' (D2D only)")

        contact = recip.get("recipient")
        if contact is not None and str(contact).strip():
            c = str(contact).strip()
            if "@" not in c or "." not in c.split("@")[-1]:
                raise ValueError(f"Recipient '{name}': invalid contact email in 'recipient': {contact!r}")

        token_expiry = recip.get("token_expiry")
        if token_expiry is not None:
            try:
                te = int(token_expiry)
                if te < 0:
                    raise ValueError(f"Recipient '{name}': token_expiry must be non-negative (got: {te})")
            except TypeError:
                raise ValueError(f"Recipient '{name}': token_expiry must be a number (got: {token_expiry!r})")

        for list_field in ("recipient_ips", "recipient_ips_to_add", "recipient_ips_to_remove"):
            val = recip.get(list_field)
            if val is not None and not isinstance(val, list):
                raise ValueError(f"Recipient '{name}': '{list_field}' must be a list (got: {type(val).__name__})")

    for idx, share_config in enumerate(shares):
        if not isinstance(share_config, dict):
            raise ValueError(f"Share at index {idx} must be a dictionary")

        share_name = share_config.get("name")
        if not share_name or not str(share_name).strip():
            raise ValueError(f"Share at index {idx} must have a non-empty 'name' field")
        share_name = str(share_name).strip()

        share_assets = share_config.get("share_assets", [])
        if not isinstance(share_assets, list):
            raise ValueError(f"Share '{share_name}': 'share_assets' must be a list")

        for asset in share_assets:
            if not asset or not str(asset).strip():
                raise ValueError(f"Share '{share_name}': share_assets must not contain empty strings")
            parts = str(asset).strip().split(".")
            if not parts or any(not p.strip() for p in parts):
                raise ValueError(
                    f"Share '{share_name}': invalid asset '{asset}' (use qualified names e.g. catalog.schema.table)"
                )

        pipelines = share_config.get("pipelines", [])
        if not isinstance(pipelines, list):
            raise ValueError(f"Share '{share_name}': 'pipelines' must be a list")

        if pipelines:
            delta_share = share_config.get("delta_share")
            if not isinstance(delta_share, dict):
                raise ValueError(
                    f"Share '{share_name}' has pipelines but missing or invalid 'delta_share' section. "
                    "delta_share must contain ext_catalog_name and ext_schema_name."
                )
            ext_catalog = (
                delta_share.get("ext_catalog_name") if isinstance(delta_share.get("ext_catalog_name"), str) else ""
            )
            ext_schema = (
                delta_share.get("ext_schema_name") if isinstance(delta_share.get("ext_schema_name"), str) else ""
            )
            if not ext_catalog or not ext_catalog.strip():
                raise ValueError(
                    f"Share '{share_name}': delta_share.ext_catalog_name is required when pipelines are defined"
                )
            if not ext_schema or not ext_schema.strip():
                raise ValueError(
                    f"Share '{share_name}': delta_share.ext_schema_name is required when pipelines are defined"
                )
            # Validate recipients (support both declarative and explicit approaches)
            recipients = share_config.get("recipients")
            recipients_to_add = share_config.get("recipients_to_add")
            recipients_to_remove = share_config.get("recipients_to_remove")

            # Check that at least one recipient field is valid
            if recipients is not None and not isinstance(recipients, list):
                raise ValueError(f"Share '{share_name}': 'recipients' must be a list (can be empty)")
            if recipients_to_add is not None and not isinstance(recipients_to_add, list):
                raise ValueError(f"Share '{share_name}': 'recipients_to_add' must be a list")
            if recipients_to_remove is not None and not isinstance(recipients_to_remove, list):
                raise ValueError(f"Share '{share_name}': 'recipients_to_remove' must be a list")
            for pipe in pipelines:
                if not isinstance(pipe, dict):
                    continue
                pipeline_name = pipe.get("name_prefix", f"<index {pipelines.index(pipe)}>")
                schedule = pipe.get("schedule")
                is_continuous = False
                has_cron = False
                if isinstance(schedule, str) and str(schedule).strip().lower() == "continuous":
                    is_continuous = True
                elif isinstance(schedule, dict):
                    if schedule.get("cron") and str(schedule.get("cron", "")).strip():
                        has_cron = True
                    else:
                        schedule_keys = [k for k in schedule.keys() if k not in ("cron", "timezone")]
                        if len(schedule_keys) == 1:
                            nested = schedule.get(schedule_keys[0])
                            if isinstance(nested, str) and str(nested).strip().lower() == "continuous":
                                is_continuous = True
                            elif isinstance(nested, dict) and nested.get("cron"):
                                has_cron = True
                if not is_continuous and not has_cron:
                    raise ValueError(
                        f"Pipeline '{pipeline_name}' in share '{share_name}' is not continuous but has no schedule info. "
                        "Please provide schedule details: either schedule.cron and schedule.timezone (v2.0), "
                        "or schedule.<source_asset>.cron and timezone (v1.0), or set schedule to 'continuous'."
                    )

        if not share_assets:
            logger.debug(f"Share '{share_name}' has no share_assets, skipping pipeline validation")
            continue

        pipeline_source_assets = set()
        for pipeline in pipelines:
            if isinstance(pipeline, dict):
                source_asset = pipeline.get("source_asset")
                if source_asset and str(source_asset).strip():
                    pipeline_source_assets.add(str(source_asset).strip())

        missing_pipelines = []
        for asset in share_assets:
            a = str(asset).strip()
            if a not in pipeline_source_assets:
                missing_pipelines.append(a)
                logger.error(f"✗ Share asset '{a}' in share '{share_name}' has no corresponding pipeline")
            else:
                logger.info(f"✓ Share asset '{a}' has pipeline")

        if missing_pipelines:
            error_msg = (
                f"Validation failed for share '{share_name}': "
                f"{len(missing_pipelines)} share asset(s) do not have corresponding pipelines.\n"
                f"Missing pipelines for: {', '.join(missing_pipelines)}\n\n"
                f"REQUIREMENT: When share_assets are specified, every asset must have a pipeline "
                f"with that asset as 'source_asset'.\n\n"
                f"Please provide pipeline details for the missing asset(s): add a 'pipelines' entry "
                f"with name_prefix, source_asset (matching the asset), schedule (cron or continuous), "
                f"and other pipeline config; or remove the asset(s) from share_assets."
            )
            raise ValueError(error_msg)

    logger.info("=" * 80)
    logger.info("✓ ALL SHAREPACK VALIDATIONS PASSED")
    logger.info("=" * 80)


def validate_metadata(metadata: Dict[str, Any]) -> None:
    """
    Validate metadata section before provisioning starts.

    Performs comprehensive validation:
    1. Email format validation (all email fields)
    2. Delta share region check (AM or EMEA)
    3. Approver status must be 'approved'
    4. ServiceNow ticket required
    5. Workspace URL reachability check

    Args:
        metadata: Metadata dictionary from share pack config

    Raises:
        ValueError: If any validation fails
    """
    logger.info("=" * 80)
    logger.info("METADATA VALIDATION")
    logger.info("=" * 80)

    # 1. Validate emails (already validated by Pydantic, but double-check for runtime safety)
    email_fields = ["requestor", "contact_email", "configurator", "approver", "executive_team"]
    for field in email_fields:
        if field in metadata:
            value = metadata[field]
            emails = [email.strip() for email in value.split(",")]
            for email in emails:
                if not email:
                    continue
                if "@" not in email or "." not in email.split("@")[-1]:
                    raise ValueError(f"Invalid email in {field}: {email}")
            logger.info(f"✓ {field}: {value}")

    # 2. Validate delta_share_region (AM or EMEA)
    region = metadata.get("delta_share_region", "").upper()
    if region not in ("AM", "EMEA"):
        raise ValueError(f"delta_share_region must be AM or EMEA, got: {region}")
    logger.info(f"✓ delta_share_region: {region}")

    # 3. Validate approver_status is 'approved'
    approver_status = metadata.get("approver_status", "").lower()
    if approver_status != "approved":
        raise ValueError(
            f"Cannot proceed with provisioning: approver_status is '{approver_status}' "
            f"(must be 'approved'). Current status indicates the request has not been approved."
        )
    logger.info(f"✓ approver_status: {approver_status}")

    # 4. Validate ServiceNow ticket is provided
    servicenow_ticket = metadata.get("servicenow_ticket") or metadata.get("servicenow")
    if not servicenow_ticket or not str(servicenow_ticket).strip():
        raise ValueError("ServiceNow ticket number or link is required for provisioning")
    logger.info(f"✓ servicenow_ticket: {servicenow_ticket}")

    # 5. Validate workspace_url is reachable
    workspace_url = metadata.get("workspace_url", "")
    if not workspace_url:
        raise ValueError("workspace_url is required")

    # Check URL format
    if not workspace_url.startswith("https://"):
        raise ValueError(f"workspace_url must be HTTPS: {workspace_url}")

    # Parse URL and validate domain
    try:
        parsed = urlparse(workspace_url)
        hostname = parsed.hostname

        # Check for valid Databricks patterns
        valid_patterns = [
            ".azuredatabricks.net",
            ".cloud.databricks.com",
            ".gcp.databricks.com",
        ]

        if not any(hostname.endswith(pattern) for pattern in valid_patterns):
            raise ValueError(f"workspace_url does not match valid Databricks patterns: {workspace_url}")

        # Check reachability (HEAD request with timeout)
        logger.info(f"Checking workspace URL reachability: {workspace_url}")
        try:
            response = requests.head(workspace_url, timeout=10, allow_redirects=True)
            if response.status_code >= 500:
                raise ValueError(f"Workspace URL returned server error (HTTP {response.status_code}): {workspace_url}")
            logger.info(f"✓ workspace_url is reachable: {workspace_url} (HTTP {response.status_code})")
        except requests.exceptions.Timeout:
            raise ValueError(f"Workspace URL timed out (not reachable): {workspace_url}")
        except requests.exceptions.ConnectionError:
            raise ValueError(f"Workspace URL connection failed (not reachable): {workspace_url}")
        except requests.exceptions.RequestException as e:
            # Allow other HTTP errors (401, 403, etc.) as they indicate the server is reachable
            logger.warning(f"Workspace URL returned HTTP error but is reachable: {e}")
            logger.info(f"✓ workspace_url is reachable (server responded): {workspace_url}")

    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Invalid workspace_url: {e}")

    # 6. Validate authentication token works for this workspace
    logger.info("Validating authentication token for workspace...")
    try:
        from datetime import datetime
        from datetime import timezone

        from databricks.sdk import WorkspaceClient

        from dbrx_api.dbrx_auth.token_gen import get_auth_token

        # Generate token
        session_token = get_auth_token(datetime.now(timezone.utc))[0]

        # Create client and test with a lightweight API call
        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        # Test authentication by listing recipients (limit 1 for speed)
        # This verifies: token is valid, workspace recognizes token, service principal has permissions
        try:
            list(w_client.recipients.list(max_results=1))
            logger.info(f"✓ Authentication token verified for workspace: {workspace_url}")
        except Exception as auth_error:
            auth_msg = str(auth_error)
            if "Invalid Token" in auth_msg or "400" in auth_msg:
                raise ValueError(
                    f"Authentication token does not work for workspace {workspace_url}.\n"
                    f"  Common causes:\n"
                    f"  1. Token generated for different account (check client_id/account_id in .env)\n"
                    f"  2. Service principal not added to this workspace\n"
                    f"  3. Wrong workspace URL in YAML metadata\n"
                    f"  Original error: {auth_msg}"
                )
            elif "403" in auth_msg or "PermissionDenied" in auth_msg:
                raise ValueError(
                    f"Service principal lacks permissions in workspace {workspace_url}.\n"
                    f"  Required: 'Metastore Admin' or 'Account Admin' role\n"
                    f"  Original error: {auth_msg}"
                )
            else:
                # Re-raise other errors
                raise ValueError(f"Authentication test failed for {workspace_url}: {auth_msg}")

    except ValueError:
        raise
    except Exception as test_error:
        logger.warning(f"Could not test authentication (proceeding anyway): {test_error}")
        # Don't fail on import errors or other unexpected issues - just warn

    logger.info("=" * 80)
    logger.info("✓ ALL METADATA VALIDATIONS PASSED")
    logger.info("=" * 80)


async def provision_sharepack_new(pool, share_pack: Dict[str, Any]):
    """
    Provision a share pack using NEW strategy (create all entities from scratch).

    This implementation calls actual Databricks APIs.

    Args:
        pool: asyncpg connection pool
        share_pack: Share pack dict from database (includes config as JSONB)

    Raises:
        Exception: If provisioning fails
    """
    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)

    # Initialize repositories
    recipient_repo = RecipientRepository(pool)
    share_repo = ShareRepository(pool)
    pipeline_repo = PipelineRepository(pool)

    # Track created resources for rollback
    created_resources = {
        "recipients": [],
        "shares": [],
        "pipelines": [],
    }

    current_step = ""
    recipient_rollback_list = []
    recipient_db_entries = []
    share_rollback_list = []
    share_db_entries = []
    pipeline_rollback_list = []
    pipeline_db_entries = []

    try:
        import json

        # Parse config if it's a JSON string
        config = share_pack["config"]
        if isinstance(config, str):
            config = json.loads(config)

        workspace_url = config["metadata"]["workspace_url"]

        logger.info(f"Starting NEW strategy provisioning for {share_pack_id}")
        logger.info(f"Target workspace: {workspace_url}")

        # Validate metadata before proceeding
        current_step = "Step 0/9: Validating metadata and configuration"
        await tracker.update(current_step)
        validate_metadata(config["metadata"])
        validate_sharepack_config(config)

        # Step 1: Initialize and detect scope
        current_step = "Step 1/9: Initializing provisioning"
        await tracker.update(current_step)

        has_recipients = bool(config.get("recipient"))
        has_shares = bool(config.get("share"))
        logger.info(f"Provisioning scope: recipients={has_recipients}, shares={has_shares}")

        # Step 2: Ensure recipients (Databricks only — no DB writes)
        if has_recipients:
            current_step = "Step 2/9: Creating/updating recipients"
            await tracker.update(current_step)
            await ensure_recipients(
                workspace_url=workspace_url,
                recipients_config=config["recipient"],
                rollback_list=recipient_rollback_list,
                db_entries=recipient_db_entries,
                created_resources=created_resources,
            )
        else:
            logger.info("No recipients in config - skipping recipient provisioning")
            await tracker.update("Step 2/9: Skipping recipients (not in config)")

        # Step 3: Validate recipient references in shares
        if has_shares:
            current_step = "Step 3/9: Validating recipient references"
            await tracker.update(current_step)
            logger.info("Validating that all recipients referenced in shares exist...")

            # Get all recipients declared in YAML
            yaml_recipients = {r["name"] for r in config["recipient"]}

            # Get all recipients referenced in shares
            referenced_recipients = set()
            for share_config in config["share"]:
                referenced_recipients.update(share_config.get("recipients", []))

            # Find recipients not in YAML
            unknown_recipients = referenced_recipients - yaml_recipients

            if unknown_recipients:
                logger.info(f"Found {len(unknown_recipients)} recipient(s) not in YAML, checking Databricks...")
                from dbrx_api.dltshr.recipient import get_recipients

                # Check if each unknown recipient exists in Databricks
                missing_recipients = []
                for recipient_name in unknown_recipients:
                    try:
                        logger.debug(f"Checking if recipient '{recipient_name}' exists in Databricks...")
                        existing = get_recipients(recipient_name, workspace_url)
                        if existing:
                            logger.info(f"✓ Recipient '{recipient_name}' found in Databricks (not in YAML)")
                        else:
                            logger.error(f"✗ Recipient '{recipient_name}' not found in YAML or Databricks")
                            missing_recipients.append(recipient_name)
                    except Exception as check_error:
                        logger.warning(f"Could not verify recipient '{recipient_name}' in Databricks: {check_error}")
                        logger.warning(f"Assuming recipient '{recipient_name}' exists (verification failed)")
                        # Don't add to missing_recipients - assume it exists if verification fails

                if missing_recipients:
                    error_msg = (
                        f"The following recipients are referenced in shares but do not exist:\n"
                        f"  Missing recipients: {', '.join(missing_recipients)}\n\n"
                        f"These recipients are:\n"
                        f"  - NOT declared in your YAML recipient section\n"
                        f"  - NOT found in Databricks workspace\n\n"
                        f"Please either:\n"
                        f"  1. Add them to the 'recipient' section in your YAML, OR\n"
                        f"  2. Create them in Databricks first, OR\n"
                        f"  3. Remove the references from your shares"
                    )
                    raise ValueError(error_msg)

            logger.success("All recipient references validated successfully")
        else:
            await tracker.update("Step 3/9: Skipping recipient validation (no shares)")

        # Step 4: Create/update shares (Databricks only — no DB writes)
        if has_shares:
            current_step = "Step 4/9: Creating/updating shares"
            await tracker.update(current_step)
            await ensure_shares(
                workspace_url=workspace_url,
                shares_config=config["share"],
                rollback_list=share_rollback_list,
                db_entries=share_db_entries,
                created_resources=created_resources,
            )
        else:
            logger.info("No shares in config - skipping share provisioning")
            await tracker.update("Step 4/9: Skipping shares (not in config)")

        # Step 5/6: Ensure pipelines and schedules (Databricks only — no DB writes)
        if has_shares:
            current_step = "Step 5/9: Creating/updating DLT pipelines and schedules"
            await tracker.update(current_step)
            await ensure_pipelines(
                workspace_url=workspace_url,
                shares_config=config["share"],
                rollback_list=pipeline_rollback_list,
                db_entries=pipeline_db_entries,
                created_resources=created_resources,
            )
        else:
            await tracker.update("Step 5/9: Skipping pipelines (no shares in config)")

        # Step 6: ALL Databricks ops succeeded → persist to DB
        current_step = "Step 6/9: Persisting to database"
        await tracker.update(current_step)

        configurator = config["metadata"]["configurator"]
        if recipient_db_entries:
            await persist_recipients_to_db(recipient_db_entries, share_pack_id, configurator, recipient_repo)
        share_name_to_id = {}
        if share_db_entries:
            share_name_to_id = await persist_shares_to_db(share_db_entries, share_pack_id, share_repo)
        if pipeline_db_entries:
            await persist_pipelines_to_db(
                pipeline_db_entries, share_pack_id, share_name_to_id, share_repo, pipeline_repo
            )

        # Step 7: Clean up orphaned pipelines (whose assets were removed from shares)
        # NOTE: Cleanup only makes sense for UPDATE strategy where assets might be removed.
        # For NEW strategy, everything is brand new - there can't be orphaned pipelines.
        logger.info("Skipping pipeline cleanup for NEW strategy (no assets removed)")
        await tracker.update("Step 7/9: Skipping pipeline cleanup (NEW strategy - not applicable)")

        # Step 8: Determine if any changes were made
        # Check if all db_entries have action='unchanged' (no changes)
        all_unchanged = True
        total_created = 0
        total_updated = 0

        for entry in recipient_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        for entry in share_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        for entry in pipeline_db_entries:
            action = entry.get("action", "")
            if action == "created":
                total_created += 1
                all_unchanged = False
            elif action == "updated":
                total_updated += 1
                all_unchanged = False

        # Step 9: Mark as completed with appropriate message
        if all_unchanged:
            completion_message = "Already up to date with share pack data"
            logger.info(f"Share pack {share_pack_id} is already up to date - no changes needed")
        else:
            completion_message = f"All steps completed successfully ({total_created} created, {total_updated} updated)"
            logger.success(f"Share pack {share_pack_id} provisioned successfully")
            logger.info(
                f"Created {len(created_resources['recipients'])} recipients, "
                f"{len(created_resources['shares'])} shares, "
                f"{len(created_resources['pipelines'])} pipelines"
            )

        await tracker.complete(completion_message)

    except Exception as e:
        await tracker.fail(str(e), current_step or "Provisioning failed")
        logger.error(f"Provisioning failed for {share_pack_id}: {e}", exc_info=True)
        logger.warning(f"Resources created before failure: {created_resources}")

        # Rollback Databricks only — no DB cleanup needed (DB was never written)
        if pipeline_rollback_list:
            logger.info("Rolling back pipeline changes in Databricks")
            try:
                _rollback_pipelines(pipeline_rollback_list, workspace_url)
            except Exception as rb_err:
                logger.error(f"Pipeline rollback failed: {rb_err}", exc_info=True)

        if share_rollback_list:
            logger.info("Rolling back share changes in Databricks")
            try:
                _rollback_shares(share_rollback_list, workspace_url)
            except Exception as rb_err:
                logger.error(f"Share rollback failed: {rb_err}", exc_info=True)

        if recipient_rollback_list:
            logger.info("Rolling back recipient changes in Databricks")
            try:
                _rollback_recipients(recipient_rollback_list, workspace_url)
            except Exception as rb_err:
                logger.error(f"Recipient rollback failed: {rb_err}", exc_info=True)

        raise
