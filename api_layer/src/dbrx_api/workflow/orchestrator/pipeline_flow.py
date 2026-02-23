"""
Unified pipeline provisioning for workflow (strategy-agnostic).

- If pipeline exists: update configuration and schedule; record for rollback (created + updated).
- If pipeline does not exist: create pipeline, create schedule if non-continuous; record for rollback.
- On any failure: roll back all pipeline changes (delete created, restore updated) and raise.
"""

import copy
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4

from loguru import logger

from dbrx_api.jobs.dbrx_pipelines import create_pipeline
from dbrx_api.jobs.dbrx_pipelines import delete_pipeline
from dbrx_api.jobs.dbrx_pipelines import find_pipelines_by_source_and_target
from dbrx_api.jobs.dbrx_pipelines import get_pipeline_by_name
from dbrx_api.jobs.dbrx_pipelines import list_pipelines_with_search_criteria
from dbrx_api.jobs.dbrx_pipelines import update_pipeline_target_configuration
from dbrx_api.jobs.dbrx_schedule import create_schedule_for_pipeline
from dbrx_api.jobs.dbrx_schedule import delete_schedule_for_pipeline
from dbrx_api.jobs.dbrx_schedule import list_schedules
from dbrx_api.jobs.dbrx_schedule import update_schedule_for_pipeline
from dbrx_api.jobs.dbrx_schedule import update_timezone_for_schedule
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository


def _resolve_source_asset(pipeline_config: Dict[str, Any], pipeline_name: str) -> str:
    """Extract source_asset from v2.0 (source_asset) or v1.0 (schedule key)."""
    source_asset = pipeline_config.get("source_asset")
    if source_asset and str(source_asset).strip():
        return str(source_asset).strip()
    schedule = pipeline_config.get("schedule", {})
    if isinstance(schedule, dict):
        schedule_keys = [k for k in schedule.keys() if k not in ("cron", "timezone", "action")]
        if len(schedule_keys) == 1:
            return str(schedule_keys[0]).strip()
    raise ValueError(
        f"Pipeline '{pipeline_name}': Cannot determine source_asset. "
        "Use v2.0 format with explicit source_asset or v1.0 schedule with single asset key."
    )


def _extract_cron_timezone(
    pipeline_config: Dict[str, Any],
    pipeline_name: str,
) -> Tuple[str, str]:
    """Extract (cron_expression, timezone) from schedule; empty cron if continuous."""
    schedule = pipeline_config.get("schedule")
    if isinstance(schedule, str) and str(schedule).strip().lower() == "continuous":
        return "", "UTC"
    if isinstance(schedule, dict):
        cron = schedule.get("cron") or ""
        tz = schedule.get("timezone", "UTC") or "UTC"
        if not cron:
            schedule_keys = [k for k in schedule.keys() if k not in ("cron", "timezone", "action")]
            if len(schedule_keys) == 1:
                nested = schedule.get(schedule_keys[0])
                if isinstance(nested, dict):
                    cron = nested.get("cron", "")
                    tz = nested.get("timezone", "UTC") or "UTC"
        return (cron.strip(), tz)
    return "", "UTC"


def _config_dict_to_list(config: Dict[str, Any]) -> List[Dict[str, str]]:
    """Convert configuration dict to list of key-value for pipelines.update API."""
    if not config:
        return []
    return [{"key": k, "value": str(v)} for k, v in config.items()]


def _rollback_pipelines(
    rollback_list: List[Tuple[str, ...]],
    workspace_url: str,
) -> None:
    """Roll back pipeline changes in reverse order. 'created' -> delete; 'updated' -> restore."""
    for item in reversed(rollback_list):
        kind = item[0]
        _ws = item[1]
        _pipeline_id = item[2]
        _pipeline_name = item[3]

        if kind == "created":
            try:
                del_result = delete_schedule_for_pipeline(
                    dltshr_workspace_url=_ws,
                    pipeline_id=_pipeline_id,
                )
                if isinstance(del_result, str) and "error" in del_result.lower():
                    logger.warning(f"Rollback: schedule delete for {_pipeline_name}: {del_result}")
                else:
                    logger.info(f"Rollback: deleted schedule(s) for pipeline {_pipeline_name}")
            except Exception as e:
                logger.error(f"Rollback: failed to delete schedule for {_pipeline_name}: {e}")
            try:
                result = delete_pipeline(dltshr_workspace_url=_ws, pipeline_id=_pipeline_id)
                if result is not None:
                    logger.error(f"Rollback: failed to delete pipeline {_pipeline_name}: {result}")
                else:
                    logger.info(f"Rollback: deleted pipeline {_pipeline_name}")
            except Exception as e:
                logger.error(f"Rollback: failed to delete pipeline {_pipeline_name}: {e}")
            continue

        if kind == "updated":
            # item: ("updated", ws, pipeline_id, pipeline_name, prev_config, prev_catalog,
            #        prev_target, prev_libraries, prev_notifications, prev_tags, prev_serverless,
            #        prev_job_id, prev_cron, prev_timezone)
            if len(item) < 14:
                logger.warning(f"Rollback: invalid 'updated' tuple for {_pipeline_name}, skipping")
                continue
            prev_config = item[4]
            prev_catalog = item[5]
            prev_target = item[6]
            prev_libraries = item[7]
            prev_notifications = item[8]
            prev_tags = item[9]
            prev_serverless = item[10]
            prev_job_id = item[11]
            prev_cron = item[12]
            prev_timezone = item[13]
            config_list = (
                _config_dict_to_list(prev_config) if isinstance(prev_config, dict) else list(prev_config or [])
            )
            try:
                res = update_pipeline_target_configuration(
                    dltshr_workspace_url=_ws,
                    pipeline_id=_pipeline_id,
                    pipeline_name=_pipeline_name,
                    configuration=config_list,
                    catalog=prev_catalog,
                    target=prev_target,
                    libraries=prev_libraries,
                    notifications=prev_notifications,
                    tags=prev_tags,
                    serverless=prev_serverless,
                )
                if isinstance(res, str):
                    logger.error(f"Rollback: failed to restore pipeline {_pipeline_name}: {res}")
                else:
                    logger.info(f"Rollback: restored pipeline config for {_pipeline_name}")
            except Exception as e:
                logger.error(f"Rollback: failed to restore pipeline {_pipeline_name}: {e}")
            if prev_job_id and (prev_cron or prev_timezone):
                try:
                    update_schedule_for_pipeline(
                        dltshr_workspace_url=_ws,
                        job_id=prev_job_id,
                        cron_expression=prev_cron or "",
                    )
                    update_timezone_for_schedule(
                        dltshr_workspace_url=_ws,
                        job_id=prev_job_id,
                        time_zone=prev_timezone or "UTC",
                    )
                    logger.info(f"Rollback: restored schedule for pipeline {_pipeline_name}")
                except Exception as e:
                    logger.error(f"Rollback: failed to restore schedule for {_pipeline_name}: {e}")


def _create_pipeline_and_schedule(
    workspace_url: str,
    share_name: str,
    share_config: Dict[str, Any],
    pipeline_config: Dict[str, Any],
    pipeline_name: str,
    created_resources: Dict[str, List],
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Create pipeline and optional schedule. Returns (databricks_pipeline_id, db_entry) or (None, None) on skip.
    Raises on failure. Does not append to rollback list (caller does).
    """
    delta_share_config = share_config.get("delta_share") or {}
    source_asset = _resolve_source_asset(pipeline_config, pipeline_name)
    target_asset = pipeline_config.get("target_asset") or source_asset.split(".")[-1]

    target_catalog = pipeline_config.get("ext_catalog_name") or delta_share_config.get("ext_catalog_name")
    target_schema = pipeline_config.get("ext_schema_name") or delta_share_config.get("ext_schema_name")
    if not target_catalog or not target_schema:
        raise ValueError(f"Pipeline '{pipeline_name}': delta_share must contain ext_catalog_name and ext_schema_name.")

    configuration = {
        "pipelines.source_table": source_asset,
        "pipelines.target_table": target_asset,
        "pipelines.keys": pipeline_config.get("key_columns", ""),
        "pipelines.scd_type": pipeline_config.get("scd_type", "2"),
    }

    result = create_pipeline(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
        target_catalog_name=target_catalog,
        target_schema_name=target_schema,
        configuration=configuration,
        notifications_list=pipeline_config.get("notification", []),
        tags=pipeline_config.get("tags", {}),
        serverless=pipeline_config.get("serverless", False),
    )

    if isinstance(result, str):
        err_lower = result.lower()
        if any(k in err_lower for k in ["already exists", "already present", "duplicate"]):
            logger.warning(f"Pipeline {pipeline_name} already exists, treating as update")
            pipelines_list = list_pipelines_with_search_criteria(
                dltshr_workspace_url=workspace_url,
                filter_expr=pipeline_name,
            )
            existing_pipeline_id = None
            for p in pipelines_list:
                if p.name == pipeline_name:
                    existing_pipeline_id = p.pipeline_id
                    break
            if not existing_pipeline_id:
                raise RuntimeError(f"Pipeline '{pipeline_name}' reported as existing but could not be found")

            cron_expr, tz = _extract_cron_timezone(pipeline_config, pipeline_name)
            db_entry = {
                "action": "already_exists",
                "share_name": share_name,
                "pipeline_name": pipeline_name,
                "databricks_pipeline_id": existing_pipeline_id,
                "asset_name": target_asset,
                "source_table": source_asset,
                "target_table": target_asset,
                "scd_type": pipeline_config.get("scd_type", "2"),
                "key_columns": pipeline_config.get("key_columns", ""),
                "schedule_type": "CRON" if cron_expr else "CONTINUOUS",
                "cron_expression": cron_expr,
                "timezone": tz,
                "serverless": pipeline_config.get("serverless", False),
                "tags": pipeline_config.get("tags"),
                "notification_emails": pipeline_config.get("notification", []),
            }

            return existing_pipeline_id, db_entry
        raise RuntimeError(f"Failed to create pipeline {pipeline_name}: {result}")

    pipeline_id = result.pipeline_id
    created_resources["pipelines"].append(pipeline_name)

    cron_expr, timezone = _extract_cron_timezone(pipeline_config, pipeline_name)
    if cron_expr:
        job_name = f"{pipeline_name}_schedule"
        schedule_result = create_schedule_for_pipeline(
            dltshr_workspace_url=workspace_url,
            job_name=job_name,
            pipeline_id=pipeline_id,
            cron_expression=cron_expr,
            time_zone=timezone,
            paused=False,
            email_notifications=pipeline_config.get("notification", []),
            tags=pipeline_config.get("tags", {}),
            description=pipeline_config.get("description"),
        )
        if isinstance(schedule_result, str):
            if "already exists" in schedule_result.lower():
                logger.info(
                    "Schedule %s already exists for %s, updating cron/timezone",
                    job_name,
                    pipeline_name,
                )
                schedules_after, _ = list_schedules(
                    dltshr_workspace_url=workspace_url,
                    pipeline_id=pipeline_id,
                )
                if schedules_after:
                    update_schedule_for_pipeline(
                        dltshr_workspace_url=workspace_url,
                        job_id=schedules_after[0]["job_id"],
                        cron_expression=cron_expr,
                    )
                    update_timezone_for_schedule(
                        dltshr_workspace_url=workspace_url,
                        job_id=schedules_after[0]["job_id"],
                        time_zone=timezone,
                    )
                    created_resources.setdefault("schedules", []).append(f"{pipeline_name} (schedule updated)")
                else:
                    created_resources.setdefault("schedules", []).append(f"{pipeline_name} (created)")
            elif "error" in schedule_result.lower():
                raise RuntimeError(f"Failed to create schedule for {pipeline_name}: {schedule_result}")
            else:
                created_resources.setdefault("schedules", []).append(f"{pipeline_name} (created)")
        else:
            created_resources.setdefault("schedules", []).append(f"{pipeline_name} (created)")

    pipeline_id_db = uuid4()
    db_entry = {
        "action": "created",
        "pipeline_id": pipeline_id_db,
        "share_name": share_name,
        "pipeline_name": pipeline_name,
        "databricks_pipeline_id": pipeline_id,
        "asset_name": target_asset,
        "source_table": source_asset,
        "target_table": target_asset,
        "scd_type": pipeline_config.get("scd_type", "2"),
        "key_columns": pipeline_config.get("key_columns", ""),
        "schedule_type": "CRON" if cron_expr else "CONTINUOUS",
        "cron_expression": cron_expr,
        "timezone": timezone,
        "serverless": pipeline_config.get("serverless", False),
        "tags": pipeline_config.get("tags", {}),
        "notification_emails": pipeline_config.get("notification", []),
    }

    return pipeline_id, db_entry


def _update_pipeline_and_schedule(
    workspace_url: str,
    pipeline_name: str,
    pipeline_id: str,
    pipeline_config: Dict[str, Any],
    share_config: Dict[str, Any],
    share_name: str,
    created_resources: Dict[str, List],
) -> Dict[str, Any]:
    """Update existing pipeline configuration and schedule; return db_entry for deferred DB write."""
    existing = get_pipeline_by_name(
        dltshr_workspace_url=workspace_url,
        pipeline_name=pipeline_name,
    )
    if not existing or not existing.spec:
        raise RuntimeError(f"Pipeline {pipeline_name} has no spec")

    existing_config = dict(existing.spec.configuration) if existing.spec.configuration else {}
    existing_source = existing_config.get("pipelines.source_table")
    new_source = pipeline_config.get("source_asset")
    if new_source and existing_source and new_source != existing_source:
        raise ValueError(
            f"Cannot change source_asset for pipeline '{pipeline_name}'. "
            "Source asset is immutable; delete and recreate to change it."
        )

    new_target = pipeline_config.get("target_asset") or (existing_source.split(".")[-1] if existing_source else "")
    new_keys = pipeline_config.get("key_columns") or existing_config.get("pipelines.keys", "")
    configuration = existing_config.copy()
    configuration["pipelines.target_table"] = new_target
    configuration["pipelines.keys"] = new_keys

    delta_share = share_config.get("delta_share", {})
    ext_catalog = (
        pipeline_config.get("ext_catalog_name")
        or delta_share.get("ext_catalog_name")
        or (existing.spec.catalog if existing.spec else None)
    )
    ext_schema = (
        pipeline_config.get("ext_schema_name")
        or delta_share.get("ext_schema_name")
        or (existing.spec.target if existing.spec else None)
    )
    libraries = existing.spec.libraries if existing.spec else None
    notifications_list = pipeline_config.get("notification", [])
    notifications = None
    if notifications_list:
        from databricks.sdk.service.pipelines import Notifications

        notifications = [
            Notifications(
                email_recipients=notifications_list,
                alerts=[
                    "on-update-failure",
                    "on-update-fatal-failure",
                    "on-update-success",
                    "on-flow-failure",
                ],
            )
        ]

    result = update_pipeline_target_configuration(
        dltshr_workspace_url=workspace_url,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        configuration=configuration,
        catalog=ext_catalog,
        target=ext_schema,
        libraries=libraries,
        notifications=notifications,
        tags=pipeline_config.get("tags"),
        serverless=pipeline_config.get("serverless"),
    )
    if isinstance(result, str):
        raise RuntimeError(f"Failed to update pipeline {pipeline_name}: {result}")

    created_resources["pipelines"].append(f"{pipeline_name} (updated)")

    cron_expr, timezone = _extract_cron_timezone(pipeline_config, pipeline_name)
    schedules, _ = list_schedules(dltshr_workspace_url=workspace_url, pipeline_id=pipeline_id)
    if cron_expr:
        if not schedules:
            job_name = f"{pipeline_name}_schedule"
            create_result = create_schedule_for_pipeline(
                dltshr_workspace_url=workspace_url,
                job_name=job_name,
                pipeline_id=pipeline_id,
                cron_expression=cron_expr,
                time_zone=timezone,
                paused=False,
                email_notifications=pipeline_config.get("notification", []),
                tags=pipeline_config.get("tags", {}),
                description=pipeline_config.get("description"),
            )
            if isinstance(create_result, str) and "already exists" in create_result.lower():
                logger.info(
                    "Schedule %s already exists for %s, updating cron/timezone",
                    job_name,
                    pipeline_name,
                )
                schedules, _ = list_schedules(
                    dltshr_workspace_url=workspace_url,
                    pipeline_id=pipeline_id,
                )
                if schedules:
                    update_schedule_for_pipeline(
                        dltshr_workspace_url=workspace_url,
                        job_id=schedules[0]["job_id"],
                        cron_expression=cron_expr,
                    )
                    update_timezone_for_schedule(
                        dltshr_workspace_url=workspace_url,
                        job_id=schedules[0]["job_id"],
                        time_zone=timezone,
                    )
            created_resources.setdefault("schedules", []).append(f"{pipeline_name} (schedule created)")
        else:
            job_id = schedules[0]["job_id"]
            update_schedule_for_pipeline(
                dltshr_workspace_url=workspace_url,
                job_id=job_id,
                cron_expression=cron_expr,
            )
            update_timezone_for_schedule(
                dltshr_workspace_url=workspace_url,
                job_id=job_id,
                time_zone=timezone,
            )
            created_resources.setdefault("schedules", []).append(f"{pipeline_name} (schedule updated)")

    source_asset = _resolve_source_asset(pipeline_config, pipeline_name)
    target_asset = pipeline_config.get("target_asset") or source_asset.split(".")[-1]
    db_entry = {
        "action": "updated",
        "share_name": share_name,
        "pipeline_name": pipeline_name,
        "databricks_pipeline_id": pipeline_id,
        "asset_name": target_asset,
        "source_table": source_asset,
        "target_table": target_asset,
        "scd_type": pipeline_config.get("scd_type", "2"),
        "key_columns": pipeline_config.get("key_columns", ""),
        "schedule_type": "CRON" if cron_expr else "CONTINUOUS",
        "cron_expression": cron_expr,
        "timezone": timezone,
        "serverless": pipeline_config.get("serverless", False),
        "tags": pipeline_config.get("tags"),
        "notification_emails": pipeline_config.get("notification", []),
    }

    return db_entry


async def ensure_pipelines(
    workspace_url: str,
    shares_config: List[Dict[str, Any]],
    rollback_list: List[Tuple[str, ...]],
    db_entries: List[Dict[str, Any]],
    created_resources: Optional[Dict[str, List]] = None,
) -> None:
    """
    Ensure all pipelines exist with desired config and schedules (strategy-agnostic).
    Databricks operations only — no DB writes. Populates mutable rollback_list and db_entries.
    On failure, raises without rollback — the orchestrator handles all rollback.

    Raises:
        Exception: On first pipeline create/update failure (orchestrator handles rollback).
    """
    if created_resources is None:
        created_resources = {"pipelines": [], "schedules": []}

    for share_config in shares_config:
        share_name = share_config.get("name", "")
        pipelines = share_config.get("pipelines", [])
        if not pipelines:
            continue

        for pipeline_config in pipelines:
            if not isinstance(pipeline_config, dict):
                continue
            pipeline_name = pipeline_config.get("name_prefix") or pipeline_config.get("name")
            if not pipeline_name:
                continue
            pipeline_name = str(pipeline_name).strip()

            logger.info(f"Ensuring pipeline: {pipeline_name}")

            existing = get_pipeline_by_name(
                dltshr_workspace_url=workspace_url,
                pipeline_name=pipeline_name,
            )

            if existing is None:
                pipeline_id, db_entry = _create_pipeline_and_schedule(
                    workspace_url=workspace_url,
                    share_name=share_name,
                    share_config=share_config,
                    pipeline_config=pipeline_config,
                    pipeline_name=pipeline_name,
                    created_resources=created_resources,
                )
                if pipeline_id:
                    rollback_list.append(("created", workspace_url, pipeline_id, pipeline_name))
                if db_entry:
                    db_entries.append(db_entry)
            else:
                pipeline_id = existing.pipeline_id
                # Capture previous state for rollback if a later step fails
                spec = existing.spec
                prev_config = copy.deepcopy(dict(spec.configuration)) if spec and spec.configuration else {}
                prev_catalog = spec.catalog if spec else None
                prev_target = spec.target if spec else None
                prev_libraries = copy.deepcopy(spec.libraries) if spec and spec.libraries else None
                prev_notifications = (
                    copy.deepcopy(spec.notifications) if spec and getattr(spec, "notifications", None) else None
                )
                prev_tags = None
                if spec and getattr(spec, "clusters", None) and len(spec.clusters) > 0:
                    ct = getattr(spec.clusters[0], "custom_tags", None)
                    prev_tags = dict(ct) if ct else None
                prev_serverless = getattr(spec, "serverless", None) if spec else None
                schedules, _ = list_schedules(
                    dltshr_workspace_url=workspace_url,
                    pipeline_id=pipeline_id,
                )
                prev_job_id = schedules[0]["job_id"] if schedules else None
                prev_cron = ""
                prev_timezone = "UTC"
                if schedules and schedules[0].get("cron_schedule"):
                    cs = schedules[0]["cron_schedule"]
                    prev_cron = cs.get("cron_expression") or ""
                    prev_timezone = cs.get("timezone") or "UTC"
                rollback_list.append(
                    (
                        "updated",
                        workspace_url,
                        pipeline_id,
                        pipeline_name,
                        prev_config,
                        prev_catalog,
                        prev_target,
                        prev_libraries,
                        prev_notifications,
                        prev_tags,
                        prev_serverless,
                        prev_job_id,
                        prev_cron,
                        prev_timezone,
                    )
                )
                db_entry = _update_pipeline_and_schedule(
                    workspace_url=workspace_url,
                    pipeline_name=pipeline_name,
                    pipeline_id=pipeline_id,
                    pipeline_config=pipeline_config,
                    share_config=share_config,
                    share_name=share_name,
                    created_resources=created_resources,
                )
                db_entries.append(db_entry)


async def delete_pipelines_for_removed_assets(
    workspace_url: str,
    share_name: str,
    removed_assets: List[str],
    pipeline_repo: Optional[PipelineRepository] = None,
    ext_catalog_name: Optional[str] = None,
    ext_schema_name: Optional[str] = None,
) -> List[str]:
    """Delete DLT pipelines (and their schedules) for assets removed from a share.

    Strategy: DB-first, Databricks API fallback.
    1. Query the DB pipeline table via pipeline_repo to find pipeline records for this
       share + source_table combination — uses the stored databricks_pipeline_id directly.
    2. For any removed assets not found in the DB, fall back to scanning Databricks pipelines
       filtered by source_table AND (if available) ext_catalog_name + ext_schema_name.

    The DB-first approach is preferred: it is fast (indexed query), reliable (no N+1 Databricks
    API calls), and scoped to this share via share_id FK. The Databricks fallback handles cases
    where the pipeline was created outside a share pack (no DB record).

    Args:
        workspace_url: Databricks workspace URL
        share_name: Share name — used to scope the DB lookup to this share only
        removed_assets: Full source asset names being removed (e.g. catalog.schema.table)
        pipeline_repo: PipelineRepository instance for DB-first lookup (optional)
        ext_catalog_name: Share's target catalog — used only for Databricks API fallback scope
        ext_schema_name: Share's target schema — used only for Databricks API fallback scope

    Returns:
        List of pipeline names that were deleted.
    """
    if not removed_assets:
        return []

    {a.strip().lower() for a in removed_assets if a}
    deleted: List[str] = []
    assets_not_in_db: List[str] = list(removed_assets)  # start with all; remove as DB matches found

    # ── Phase 1: DB-first lookup ────────────────────────────────────────────────
    if pipeline_repo is not None:
        try:
            db_pipelines = await pipeline_repo.list_by_share_name(share_name)
        except Exception as db_err:
            logger.warning(f"DB lookup for pipelines failed, will use Databricks fallback: {db_err}")
            db_pipelines = []

        assets_not_in_db = []
        for asset in removed_assets:
            asset_lower = asset.strip().lower()
            matches = [p for p in db_pipelines if (p.get("source_table") or "").strip().lower() == asset_lower]
            if not matches:
                logger.debug(f"No DB record for removed asset '{asset}' in share '{share_name}' — will try Databricks")
                assets_not_in_db.append(asset)
                continue

            for row in matches:
                dbrx_pipeline_id = row.get("databricks_pipeline_id")
                pipeline_name = row.get("pipeline_name") or dbrx_pipeline_id
                if not dbrx_pipeline_id:
                    logger.warning(
                        f"DB record for pipeline '{pipeline_name}' has no databricks_pipeline_id — skipping"
                    )
                    continue

                logger.info(f"Found pipeline '{pipeline_name}' via DB for removed asset '{asset}'")
                _delete_single_pipeline(workspace_url, dbrx_pipeline_id, pipeline_name, asset, deleted)

    # ── Phase 2: Databricks API fallback (for assets not found in DB) ───────────
    if assets_not_in_db:
        if not ext_catalog_name or not ext_schema_name:
            logger.warning(
                f"Cannot find pipelines for {len(assets_not_in_db)} removed asset(s) via Databricks API: "
                "delta_share.ext_catalog_name and ext_schema_name are required to safely scope the search "
                "to this share only. Add them to the share's delta_share config."
            )
        else:
            logger.info(
                f"Databricks fallback: searching for pipelines for {len(assets_not_in_db)} asset(s) "
                f"not found in DB: {assets_not_in_db}"
            )
            matched = find_pipelines_by_source_and_target(
                dltshr_workspace_url=workspace_url,
                source_tables=assets_not_in_db,
                ext_catalog_name=ext_catalog_name,
                ext_schema_name=ext_schema_name,
            )
            for pipeline in matched:
                dbrx_pipeline_id = pipeline.pipeline_id
                pipeline_name = pipeline.name or dbrx_pipeline_id
                source_table = (
                    (pipeline.spec.configuration or {}).get("pipelines.source_table", "unknown")
                    if pipeline.spec
                    else "unknown"
                )
                _delete_single_pipeline(workspace_url, dbrx_pipeline_id, pipeline_name, source_table, deleted)

    return deleted


def _delete_single_pipeline(
    workspace_url: str,
    pipeline_id: str,
    pipeline_name: str,
    source_table: str,
    deleted: List[str],
) -> None:
    """Delete schedule + pipeline for one pipeline; appends name to deleted on success."""
    try:
        delete_schedule_for_pipeline(dltshr_workspace_url=workspace_url, pipeline_id=pipeline_id)
        logger.info(f"Deleted schedule(s) for pipeline '{pipeline_name}'")
    except Exception as e:
        logger.warning(f"Could not delete schedule for pipeline '{pipeline_name}': {e}")

    try:
        result = delete_pipeline(dltshr_workspace_url=workspace_url, pipeline_id=pipeline_id)
        if isinstance(result, str):
            logger.warning(f"Pipeline delete warning for '{pipeline_name}': {result}")
        else:
            logger.success(f"Deleted pipeline '{pipeline_name}' (source_table={source_table})")
            deleted.append(pipeline_name)
    except Exception as e:
        logger.error(f"Failed to delete pipeline '{pipeline_name}': {e}")


async def check_and_sync_pipelines_for_added_assets(
    workspace_url: str,
    added_assets_per_share: List[Dict[str, Any]],
    pipeline_db_entries: List[Dict[str, Any]],
    pipeline_repo: Optional[PipelineRepository] = None,
) -> None:
    """Verify that every newly added share asset has a DLT pipeline.

    For each asset in added_assets_per_share:
    1. Skip if already covered by pipeline_db_entries (YAML pipeline config processed
       by ensure_pipelines).
    2. Check the DB (pipeline_repo) for an existing record matching share + source_table.
       If found, the asset is already tracked — log and skip.
    3. Check Databricks API for an existing pipeline (requires ext_catalog_name +
       ext_schema_name for safe scoping). If found, append a minimal entry to
       pipeline_db_entries so the record is created/updated in the DB write step.
    4. If not found anywhere: collect the asset as missing.

    After processing all assets, if any are missing, raises ValueError with a clear
    message listing every share + asset that needs a pipeline config. The caller's
    existing try/except rollback block handles undoing all Databricks changes.

    Raises:
        ValueError: If one or more newly added assets have no pipeline in YAML, DB,
                    or Databricks. Rolling back share/pipeline/recipient changes is the
                    caller's responsibility (handled by the provisioning orchestrators).
    """
    if not added_assets_per_share:
        return

    # Build set of (share_name, source_table_lower) already covered by YAML pipeline config
    covered: set = {
        (e.get("share_name", ""), (e.get("source_table") or "").strip().lower())
        for e in pipeline_db_entries
        if e.get("source_table")
    }

    # missing: {share_name: [asset, ...]}
    missing: Dict[str, List[str]] = {}

    for item in added_assets_per_share:
        share_name = item["share_name"]
        ext_catalog_name = (item.get("ext_catalog_name") or "").strip()
        ext_schema_name = (item.get("ext_schema_name") or "").strip()

        for asset in item["added_assets"]:
            asset_lower = asset.strip().lower()

            # ── 1. Already covered by YAML pipeline config ──────────────────
            if (share_name, asset_lower) in covered:
                logger.debug(
                    f"Asset '{asset}' added to share '{share_name}': "
                    "covered by YAML pipeline config — skipping pipeline check."
                )
                continue

            # ── 2. DB lookup ─────────────────────────────────────────────────
            db_found = False
            if pipeline_repo is not None:
                try:
                    db_pipelines = await pipeline_repo.list_by_share_name(share_name)
                    matches = [p for p in db_pipelines if (p.get("source_table") or "").strip().lower() == asset_lower]
                    if matches:
                        db_found = True
                        logger.info(
                            f"Asset '{asset}' added to share '{share_name}': "
                            f"pipeline '{matches[0].get('pipeline_name')}' already tracked in DB — no action needed."
                        )
                except Exception as db_err:
                    logger.warning(f"DB pipeline check for asset '{asset}' in share '{share_name}': {db_err}")

            if db_found:
                continue

            # ── 3. Databricks API fallback ───────────────────────────────────
            dbrx_found = False
            if ext_catalog_name and ext_schema_name:
                try:
                    matched = find_pipelines_by_source_and_target(
                        dltshr_workspace_url=workspace_url,
                        source_tables=[asset],
                        ext_catalog_name=ext_catalog_name,
                        ext_schema_name=ext_schema_name,
                    )
                    if matched:
                        dbrx_found = True
                        for pipeline in matched:
                            p_name = pipeline.name or pipeline.pipeline_id
                            src_tbl = (
                                (pipeline.spec.configuration or {}).get("pipelines.source_table", asset)
                                if pipeline.spec
                                else asset
                            )
                            logger.info(
                                f"Asset '{asset}' added to share '{share_name}': "
                                f"found existing Databricks pipeline '{p_name}'. "
                                "Adding minimal DB record for tracking."
                            )
                            # Append to pipeline_db_entries so persist_pipelines_to_db creates a DB record
                            pipeline_db_entries.append(
                                {
                                    "action": "updated",
                                    "pipeline_name": p_name,
                                    "pipeline_id": None,
                                    "databricks_pipeline_id": pipeline.pipeline_id,
                                    "share_name": share_name,
                                    "source_table": src_tbl,
                                    "asset_name": src_tbl.split(".")[-1] if "." in src_tbl else src_tbl,
                                    "target_table": (pipeline.spec.target or "") if pipeline.spec else "",
                                    "scd_type": "2",
                                    "key_columns": "",
                                    "schedule_type": "CRON",
                                    "cron_expression": "",
                                    "timezone": "UTC",
                                    "serverless": bool(pipeline.spec.serverless) if pipeline.spec else False,
                                    "tags": {},
                                    "notification_emails": [],
                                }
                            )
                except Exception as dbrx_err:
                    logger.warning(
                        f"Databricks pipeline check for asset '{asset}' in share '{share_name}': {dbrx_err}"
                    )
            else:
                logger.debug(
                    f"Asset '{asset}' added to share '{share_name}': "
                    "skipping Databricks pipeline search — ext_catalog_name and ext_schema_name "
                    "are required in the delta_share config to safely scope the search."
                )

            # ── 4. Not found anywhere ────────────────────────────────────────
            if not dbrx_found:
                missing.setdefault(share_name, []).append(asset)

    if missing:
        lines = [f"  - Share '{sn}': {', '.join(assets)}" for sn, assets in missing.items()]
        raise ValueError(
            "The following assets were added to shares but have no DLT pipeline "
            "configured in the YAML, tracked in the database, or found in Databricks.\n"
            + "\n".join(lines)
            + "\n\nPlease add a 'pipelines' section in the YAML for each asset, or ensure "
            "the pipeline already exists in Databricks (with ext_catalog_name and "
            "ext_schema_name set in the delta_share config for automatic detection).\n"
            "All share/recipient changes from this run have been rolled back."
        )
