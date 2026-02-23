"""
Share Pack Provisioning - DELETE Strategy.

DELETE config is name-only: list of recipient names, list of share names.
Optionally per-share explicit pipeline names to unschedule/delete.

- delete schedule AND pipeline for ALL pipelines related to the share, always.
- If explicit pipeline names are listed: those are deleted first, then all remaining
  pipelines for the share are also deleted. Listing only some pipeline names does NOT
  protect the rest — all pipelines belonging to a share are always fully deleted.
  To avoid confusion, either list ALL pipeline names or omit the 'pipelines' section
  entirely so that every active pipeline for the share is deleted automatically.

Order: pipelines -> shares -> recipients.
"""

import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import UUID

from loguru import logger

from dbrx_api.dltshr.recipient import delete_recipient
from dbrx_api.dltshr.recipient import get_recipients
from dbrx_api.dltshr.share import delete_share
from dbrx_api.dltshr.share import get_shares
from dbrx_api.jobs.dbrx_pipelines import delete_pipeline
from dbrx_api.jobs.dbrx_pipelines import list_pipelines_with_search_criteria
from dbrx_api.jobs.dbrx_schedule import delete_schedule_for_pipeline
from dbrx_api.workflow.db.repository_pipeline import PipelineRepository
from dbrx_api.workflow.db.repository_recipient import RecipientRepository
from dbrx_api.workflow.db.repository_share import ShareRepository
from dbrx_api.workflow.orchestrator.status_tracker import StatusTracker

# Type alias for deferred soft-delete entries: (repo, entity_id, reason)
_SoftDelete = Tuple[Any, UUID, str]


def _recipient_names_from_config(config: Dict[str, Any]) -> List[str]:
    """
    Extract flat list of recipient names.

    Accepts list of strings or list of {name}.
    """
    raw = config.get("recipient") or []
    names = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            names.append(item.strip())
        elif isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]).strip())
    return names


def _share_specs_from_config(config: Dict[str, Any]) -> List[Tuple[str, List[str]]]:
    """
    Extract (share_name, explicit_pipeline_name_prefixes) per share.

    Accepts share as list of strings (names only) or list of
    {name, pipelines?: [...]}. pipelines can be list of strings
    (name_prefix) or list of {name_prefix}.
    """
    raw = config.get("share") or []
    specs: List[Tuple[str, List[str]]] = []
    for item in raw:
        if isinstance(item, str) and item.strip():
            specs.append((item.strip(), []))
        elif isinstance(item, dict):
            name = item.get("name")
            if not name:
                continue
            pipeline_names = _extract_pipeline_names(item.get("pipelines") or [])
            specs.append((str(name).strip(), pipeline_names))
    return specs


def _extract_pipeline_names(pipelines_raw: List[Any]) -> List[str]:
    """Extract pipeline name prefixes from a raw pipeline list."""
    names = []
    for p in pipelines_raw:
        if isinstance(p, str) and p.strip():
            names.append(p.strip())
        elif isinstance(p, dict) and (p.get("name_prefix") or p.get("name")):
            names.append(str(p.get("name_prefix") or p.get("name")).strip())
    return names


def _handle_schedule_result(sch_result: Any, pipeline_name: str, action: str = "delete schedule for") -> None:
    """
    Interpret the result of a delete_schedule_for_pipeline call.

    Raises RuntimeError on error; logs info if schedule not found.
    """
    if not isinstance(sch_result, str):
        return
    lower = sch_result.lower()
    if "no schedules found" in lower or ("not found" in lower and "error" not in lower):
        logger.info("Schedule for pipeline '{}' does not exist in Databricks, skipping", pipeline_name)
    elif "error" in lower:
        raise RuntimeError(f"Failed to {action} pipeline {pipeline_name}: {sch_result}")


def _queue_pipeline_soft_delete(
    pipeline_name: str,
    pipelines_list: List[Dict[str, Any]],
    pipeline_repo: PipelineRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
    suffix: str = "",
) -> None:
    """Append a pipeline soft-delete entry if the pipeline exists in the pre-loaded list."""
    for rec in pipelines_list:
        if rec.get("pipeline_name") == pipeline_name:
            pending_soft_deletes.append(
                (pipeline_repo, rec["pipeline_id"], f"DELETE strategy: share pack {share_pack_id}{suffix}")
            )
            break


async def _sync_pipeline_db_soft_delete(
    pipeline_name: str,
    pipeline_repo: PipelineRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
    suffix: str = "",
) -> None:
    """
    Look up the pipeline by name in DB and queue a soft-delete for any record not already queued.

    Handles pipelines not in the pre-loaded pipelines_list — e.g. API-created pipelines
    with share_id=NULL, or records belonging to a different share_pack.
    include_deleted=True is intentional here: this function is specifically for queuing
    soft-deletes, so we want to find all records (active or already soft-deleted) to
    ensure stale entries are also cleaned up. Calling soft_delete on an already-deleted
    record is a no-op.
    """
    try:
        already_queued_ids: set = {eid for (_, eid, _) in pending_soft_deletes}
        records = await pipeline_repo.list_by_pipeline_name(pipeline_name, include_deleted=False)
        if not records:
            records = await pipeline_repo.list_by_pipeline_name(pipeline_name, include_deleted=True)
        for rec in records:
            pid = rec["pipeline_id"]
            if pid not in already_queued_ids:
                pending_soft_deletes.append(
                    (pipeline_repo, pid, f"DELETE strategy: share pack {share_pack_id}{suffix}")
                )
                already_queued_ids.add(pid)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug("Could not look up pipeline '{}' for DB soft-delete: {}", pipeline_name, e)


async def _delete_explicit_pipeline(
    name_prefix: str,
    workspace_url: str,
    share_id: Optional[UUID],
    pipelines_list: List[Dict[str, Any]],
    pipeline_repo: PipelineRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
) -> None:
    """
    Delete a single pipeline from Databricks and queue its DB soft-delete.

    Queues a DB soft-delete even when the pipeline is not found in Databricks.
    Always syncs the DB record regardless of how the pipeline was originally discovered
    (covers API-created pipelines with share_id=NULL that are not in pipelines_list).
    """
    dbrx_pipelines = list_pipelines_with_search_criteria(
        dltshr_workspace_url=workspace_url,
        filter_expr=name_prefix,
    )
    databricks_pipeline_id: Optional[str] = None
    for p in dbrx_pipelines:
        if p.name == name_prefix:
            databricks_pipeline_id = p.pipeline_id
            break

    if databricks_pipeline_id is None:
        logger.info("Pipeline '{}' does not exist in Databricks, skipping deletion", name_prefix)
        _queue_pipeline_soft_delete(
            name_prefix,
            pipelines_list,
            pipeline_repo,
            share_pack_id,
            pending_soft_deletes,
            suffix=" (not found in Databricks)",
        )
        # Also look up fresh — catches records not in the pre-loaded list (e.g. share_id=NULL)
        await _sync_pipeline_db_soft_delete(
            name_prefix,
            pipeline_repo,
            share_pack_id,
            pending_soft_deletes,
            suffix=" (not found in Databricks)",
        )
        return

    sch_result = delete_schedule_for_pipeline(dltshr_workspace_url=workspace_url, pipeline_id=databricks_pipeline_id)
    _handle_schedule_result(sch_result, name_prefix)

    result = delete_pipeline(dltshr_workspace_url=workspace_url, pipeline_id=databricks_pipeline_id)
    if result is not None:
        raise RuntimeError(f"Failed to delete pipeline {name_prefix}: {result}")
    logger.info("Deleted pipeline: {}", name_prefix)
    _queue_pipeline_soft_delete(name_prefix, pipelines_list, pipeline_repo, share_pack_id, pending_soft_deletes)
    # Also look up fresh — catches records not in the pre-loaded list (e.g. share_id=NULL)
    await _sync_pipeline_db_soft_delete(name_prefix, pipeline_repo, share_pack_id, pending_soft_deletes)


async def _delete_explicit_pipelines_for_share(
    explicit_pipeline_names: List[str],
    workspace_url: str,
    share_id: Optional[UUID],
    pipelines_list: List[Dict[str, Any]],
    pipeline_repo: PipelineRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
) -> None:
    """Delete all explicitly-listed pipelines for a share."""
    for name_prefix in explicit_pipeline_names:
        await _delete_explicit_pipeline(
            name_prefix,
            workspace_url,
            share_id,
            pipelines_list,
            pipeline_repo,
            share_pack_id,
            pending_soft_deletes,
        )


def _candidates_from_db(db_pipelines: List[Dict[str, Any]]) -> List[tuple]:
    """Build (pipeline_name, source_asset) candidates from pre-loaded DB records."""
    candidates: List[tuple] = []
    for pipe_rec in db_pipelines:
        pname: str = str(pipe_rec.get("pipeline_name") or "")
        if pname:
            candidates.append((pname, str(pipe_rec.get("source_table") or "")))
    return candidates


def _candidates_from_databricks(
    share_name: str,
    share_id: Optional[UUID],
    share_rec: Optional[Dict[str, Any]],
    workspace_url: str,
) -> List[tuple]:
    """
    Build (pipeline_name, source_asset) candidates by searching Databricks.

    Used as fallback when the DB has no active records for the share.

    Searches by share name as a substring of pipeline names.
    Full asset paths (e.g., 'catalog.schema.table') are NOT suitable search terms because
    Databricks filters on pipeline names — names virtually never contain full table paths.
    Pipeline names typically include the share name as a prefix or part of the name.

    If no pipelines are found via share name search, no implicit deletion is possible.
    In that case the user should add explicit pipeline names to the DELETE config.
    """
    logger.info(
        "No active DB-tracked pipelines for share '{}' (share_id={}), " "searching Databricks by share name",
        share_name,
        share_id,
    )

    candidates: List[tuple] = []
    seen_names: set = set()
    try:
        dbrx_hits = list_pipelines_with_search_criteria(
            dltshr_workspace_url=workspace_url,
            filter_expr=share_name,
        )
        for p in dbrx_hits:
            if p.name and str(p.name) not in seen_names:
                seen_names.add(str(p.name))
                candidates.append((str(p.name), ""))
    except Exception as search_err:  # pylint: disable=broad-except
        logger.warning(
            "Databricks search for share '{}' failed: {}",
            share_name,
            search_err,
        )

    if not candidates:
        logger.info(
            "No Databricks pipelines found matching share name '{}'. "
            "If pipelines exist with names that do not contain the share name, "
            "specify them explicitly in the DELETE config under 'pipelines:'.",
            share_name,
        )
    return candidates


async def _filter_candidates_to_share(
    candidates: List[tuple],
    share_id: UUID,
    pipeline_repo: PipelineRepository,
) -> List[tuple]:
    """
    Filter Databricks-discovered pipeline candidates to only those linked to share_id in the DB.

    Searching Databricks by share name can return pipeline hits from OTHER shares whose names
    also contain the share name as a substring. This step removes those cross-share hits before
    attempting deletion, so only pipelines that belong to the current share are processed.

    Only active (is_deleted=false, is_current=true) records are considered.

    Rules:
    - Active DB record with share_id == current share → keep.
    - Active DB record with share_id == different share → skip (cross-share hit).
    - No active DB record at all → keep (un-tracked pipeline; downstream guards will decide).
    """
    filtered: List[tuple] = []
    for pipeline_name, source_asset in candidates:
        records = await pipeline_repo.list_by_pipeline_name(pipeline_name, include_deleted=False)

        if not records:
            # Not in DB as active — cannot determine ownership; include for best-effort deletion
            filtered.append((pipeline_name, source_asset))
            continue

        if any(rec.get("share_id") == share_id for rec in records):
            filtered.append((pipeline_name, source_asset))
        else:
            logger.debug(
                "Pipeline '{}' found via Databricks search but belongs to a different share, skipping",
                pipeline_name,
            )
    return filtered


async def _delete_implicit_pipelines(
    share_name: str,
    share_id: Optional[UUID],
    share_id_to_pipelines: Dict[UUID, List[Dict[str, Any]]],
    workspace_url: str,
    pipeline_repo: PipelineRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
    share_rec: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Delete schedule + pipeline for all pipelines associated with a share when none are explicitly listed.

    Discovery order:
    1. Active DB records linked to this share (is_deleted=false, is_current=true).
    2. Databricks search by share name if no active DB records found.
       Pipeline names virtually never contain full table paths, so the share name is used
       as the search term. Databricks hits are filtered to only pipelines linked to this
       share in the DB (cross-share false-positives are removed).

    After finding a pipeline name, reuses _delete_explicit_pipeline which searches Databricks
    by exact name (authoritative source of truth), deletes schedule + pipeline, and queues
    a DB soft-delete.
    """
    db_pipelines: List[Dict[str, Any]] = share_id_to_pipelines.get(share_id, []) if share_id else []

    if db_pipelines:
        candidates = _candidates_from_db(db_pipelines)
    else:
        candidates = _candidates_from_databricks(share_name, share_id, share_rec, workspace_url)
        # Filter cross-share hits: Databricks search by share name can return pipelines
        # from OTHER shares. Only keep candidates that are actually linked to this share.
        if candidates and share_id:
            candidates = await _filter_candidates_to_share(candidates, share_id, pipeline_repo)

    if not candidates:
        logger.info("No pipelines found for share '{}', nothing to delete", share_name)
        return

    logger.info(
        "Implicit DELETE: processing {} pipeline(s) for share '{}'",
        len(candidates),
        share_name,
    )
    for pipeline_name, _ in candidates:
        try:
            await _delete_explicit_pipeline(
                name_prefix=pipeline_name,
                workspace_url=workspace_url,
                share_id=share_id,
                pipelines_list=list(db_pipelines),
                pipeline_repo=pipeline_repo,
                share_pack_id=share_pack_id,
                pending_soft_deletes=pending_soft_deletes,
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to delete implicit pipeline '{}': {}", pipeline_name, e)


async def _delete_share_record(
    share_name: str,
    workspace_url: str,
    share_rec: Optional[Dict[str, Any]],
    share_repo: ShareRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
) -> None:
    """Delete share from Databricks and queue DB soft-delete entry."""
    existing_share = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)
    if existing_share is None:
        logger.info("Share '{}' does not exist in Databricks, skipping deletion", share_name)
        for rec in await share_repo.list_by_share_name(share_name):
            pending_soft_deletes.append(
                (share_repo, rec["share_id"], f"DELETE strategy: share pack {share_pack_id} (not found in Databricks)")
            )
        return

    result = delete_share(share_name=share_name, dltshr_workspace_url=workspace_url)
    if result is not None:
        raise RuntimeError(f"Failed to delete share {share_name}: {result}")
    logger.info("Deleted share: {}", share_name)
    share_records = [share_rec] if share_rec else await share_repo.list_by_share_name(share_name)
    for rec in share_records:
        pending_soft_deletes.append((share_repo, rec["share_id"], f"DELETE strategy: share pack {share_pack_id}"))


async def _process_share(
    share_name: str,
    explicit_pipeline_names: List[str],
    workspace_url: str,
    share_id: Optional[UUID],
    share_rec: Optional[Dict[str, Any]],
    share_id_to_pipelines: Dict[UUID, List[Dict[str, Any]]],
    pipelines_list: List[Dict[str, Any]],
    pipeline_repo: PipelineRepository,
    share_repo: ShareRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
) -> None:
    """
    Handle pipeline cleanup and share deletion for one share entry.

    ALL pipelines for the share are always deleted — explicit or implicit.

    If explicit_pipeline_names is provided, those pipelines are deleted first.
    _delete_implicit_pipelines then runs unconditionally to catch any remaining
    pipelines not covered by the explicit list. Pipelines already deleted in the
    explicit pass are safely skipped (not found in Databricks → no-op).

    NOTE: listing only SOME pipeline names does NOT preserve the rest. If you do
    not want all pipelines deleted, do not use the DELETE strategy. To avoid
    confusion, either list ALL pipeline names explicitly or omit the 'pipelines'
    section so all are deleted automatically without ambiguity.
    """
    if explicit_pipeline_names:
        # Warn when the explicit list does not cover all DB-tracked pipelines.
        # All pipelines belonging to a share are always fully deleted — unlisted ones
        # are NOT preserved. The user should either supply ALL pipeline names or omit
        # the 'pipelines' section entirely and let the implicit pass handle everything.
        db_pipelines_for_share = share_id_to_pipelines.get(share_id, []) if share_id else []
        db_pipeline_names = {str(p["pipeline_name"]) for p in db_pipelines_for_share if p.get("pipeline_name")}
        unlisted = db_pipeline_names - set(explicit_pipeline_names)
        if unlisted:
            logger.warning(
                "Share '{}': {} pipeline(s) tracked in DB are NOT in your explicit list: {}. "
                "These will also be deleted because all pipelines for a share are always removed. "
                "To avoid this warning, either list ALL pipeline names or omit the 'pipelines' "
                "section so all pipelines are deleted automatically. Resubmit with the corrected config.",
                share_name,
                len(unlisted),
                ", ".join(sorted(unlisted)),
            )

        await _delete_explicit_pipelines_for_share(
            explicit_pipeline_names,
            workspace_url,
            share_id,
            pipelines_list,
            pipeline_repo,
            share_pack_id,
            pending_soft_deletes,
        )

    # Always run implicit deletion to ensure every remaining pipeline for this share
    # is deleted in Databricks and DB. When called after an explicit pass, pipelines
    # already deleted above are skipped gracefully (not found in Databricks → no-op).
    await _delete_implicit_pipelines(
        share_name=share_name,
        share_id=share_id,
        share_id_to_pipelines=share_id_to_pipelines,
        workspace_url=workspace_url,
        pipeline_repo=pipeline_repo,
        share_pack_id=share_pack_id,
        pending_soft_deletes=pending_soft_deletes,
        share_rec=share_rec,
    )

    await _delete_share_record(
        share_name,
        workspace_url,
        share_rec,
        share_repo,
        share_pack_id,
        pending_soft_deletes,
    )


async def _process_recipient(
    recipient_name: str,
    workspace_url: str,
    recipient_repo: RecipientRepository,
    share_pack_id: str,
    pending_soft_deletes: List[_SoftDelete],
) -> None:
    """Delete a single recipient from Databricks and queue its DB soft-delete."""
    existing = get_recipients(recipient_name, workspace_url)
    if existing is None:
        logger.info("Recipient '{}' does not exist in Databricks, skipping deletion", recipient_name)
        for rec in await recipient_repo.list_by_recipient_name(recipient_name):
            pending_soft_deletes.append(
                (
                    recipient_repo,
                    rec["recipient_id"],
                    f"DELETE strategy: share pack {share_pack_id} (not found in Databricks)",
                )
            )
        return

    result = delete_recipient(recipient_name=recipient_name, dltshr_workspace_url=workspace_url)
    if isinstance(result, str):
        raise RuntimeError(f"Failed to delete recipient {recipient_name}: {result}")
    logger.info("Deleted recipient: {}", recipient_name)
    for rec in await recipient_repo.list_by_recipient_name(recipient_name):
        pending_soft_deletes.append(
            (recipient_repo, rec["recipient_id"], f"DELETE strategy: share pack {share_pack_id}")
        )


def _load_delete_config(share_pack: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Parse and validate the share pack config dict.

    Returns (workspace_url, config). Raises ValueError on invalid input.
    """
    config = share_pack["config"]
    if isinstance(config, str):
        config = json.loads(config)
    if not isinstance(config, dict):
        raise ValueError("Share pack config must be a dictionary")
    metadata = config.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata section must be a dictionary")
    workspace_url = metadata.get("workspace_url")
    if not workspace_url or not str(workspace_url).strip():
        raise ValueError("metadata.workspace_url is required for DELETE strategy")
    return str(workspace_url).strip(), config


async def _load_db_records(
    share_specs: List[Tuple[str, List[str]]],
    share_repo: ShareRepository,
    pipeline_repo: PipelineRepository,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[UUID, List[Dict[str, Any]]]]:
    """
    Load share and pipeline records from DB for all shares in the DELETE config.

    Only active records (is_deleted=false, is_current=true) are loaded.
    A soft-deleted share implies its pipelines are also considered deleted — the
    Databricks search fallback in _delete_implicit_pipelines handles the case where
    Databricks still has the pipeline even though DB records are gone.

    A DELETE share pack is brand-new and creates nothing — shares/pipelines were
    provisioned under a different share pack. We load by name/share_id across ALL
    share packs rather than filtering by this share_pack_id.

    Returns (name_to_share, pipelines_list, share_id_to_pipelines).
    """
    name_to_share: Dict[str, Dict[str, Any]] = {}
    for spec_share_name, _ in share_specs:
        records = await share_repo.list_by_share_name(spec_share_name)
        if records:
            name_to_share[spec_share_name] = records[0]

    pipelines_list: List[Dict[str, Any]] = []
    share_id_to_pipelines: Dict[UUID, List[Dict[str, Any]]] = {}
    for share_name_key, share_rec_entry in name_to_share.items():
        sid: UUID = share_rec_entry["share_id"]
        # Use list_by_share_name (subquery over all historical share_ids for this name)
        # rather than list_by_share_id, so that pipelines whose share_id FK is stale
        # (e.g. from a provisioning run before share UUID reuse was enforced) are still found.
        # Only active records (is_deleted=false) — soft-deleted pipeline = pipeline is gone.
        pips = await pipeline_repo.list_by_share_name(share_name_key)
        pipelines_list.extend(pips)
        share_id_to_pipelines[sid] = pips

    return name_to_share, pipelines_list, share_id_to_pipelines


async def _persist_soft_deletes(pending: List[_SoftDelete], share_pack_id: str) -> Tuple[int, int]:
    """
    Flush all deferred soft-deletes to the database.

    Processes every entry even if individual soft-deletes fail so that a single
    failure does not prevent other records from being updated.

    Returns (success_count, failure_count) so callers can include results in the
    share pack completion message.
    """
    success = 0
    failures = 0
    for repo, entity_id, reason in pending:
        try:
            result = await repo.soft_delete(
                entity_id,
                deleted_by="orchestrator",
                deletion_reason=reason,
                request_source="share_pack",
            )
            if result is None:
                logger.debug("Soft-delete no-op for entity {} (not found or already deleted)", entity_id)
            success += 1
        except Exception as e:  # pylint: disable=broad-except
            failures += 1
            logger.error("Failed to soft-delete entity {}: {}", entity_id, e)
    if pending:
        logger.info(
            "DB soft-delete complete for share pack {}: {} succeeded, {} failed",
            share_pack_id,
            success,
            failures,
        )
    return success, failures


def _build_delete_summary(
    share_specs: List[Tuple[str, List[str]]],
    recipient_names: List[str],
    db_ok: int,
    db_fail: int,
) -> str:
    """Build completion message summarising what was deleted."""
    explicit_pipeline_count = sum(len(pipelines) for _, pipelines in share_specs)
    parts = []
    if share_specs:
        parts.append(f"{len(share_specs)} share(s)")
    if explicit_pipeline_count:
        parts.append(f"{explicit_pipeline_count} explicit pipeline(s)")
    if recipient_names:
        parts.append(f"{len(recipient_names)} recipient(s)")
    what = ", ".join(parts) if parts else "no entities"
    summary = f"Deleted: {what}. DB records soft-deleted: {db_ok}"
    if db_fail:
        summary += f" ({db_fail} DB update(s) failed - check logs)"
    return summary


async def provision_sharepack_delete(pool: Any, share_pack: Dict[str, Any]) -> None:
    """
    Provision a share pack using DELETE strategy (name-only config).

    Config: metadata.workspace_url, recipient: [names],
    share: [names or {name, pipelines?: [...]}].

    - ALL pipelines for each share are always deleted (schedule + pipeline).
    - If explicit pipeline names are listed, those are deleted first; all remaining
      pipelines for the share are then deleted by the implicit pass.
    - Listing only some pipeline names does NOT preserve the rest — omit the
      'pipelines' section or list all names to avoid a config warning.

    A share that is soft-deleted in DB implies its pipelines are also considered
    deleted. All DB queries use is_deleted=false AND is_current=true.

    Order: pipelines -> shares -> recipients.
    """
    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)
    current_step = ""

    try:
        workspace_url, config = _load_delete_config(share_pack)
        recipient_names = _recipient_names_from_config(config)
        share_specs = _share_specs_from_config(config)
        if not recipient_names and not share_specs:
            raise ValueError(
                "DELETE strategy requires at least one recipient or one share to delete. "
                "Provide 'recipient' and/or 'share' in the config (name-only lists)."
            )

        recipient_repo = RecipientRepository(pool)
        share_repo = ShareRepository(pool)
        pipeline_repo = PipelineRepository(pool)
        pending_soft_deletes: List[_SoftDelete] = []

        current_step = "Step 1/4: Loading DB records for DELETE"
        await tracker.update(current_step)
        name_to_share, pipelines_list, share_id_to_pipelines = await _load_db_records(
            share_specs, share_repo, pipeline_repo
        )

        current_step = "Step 2/4: Deleting pipelines and shares"
        await tracker.update(current_step)
        for share_name, explicit_pipeline_names in share_specs:
            share_rec = name_to_share.get(share_name)
            share_id = share_rec["share_id"] if share_rec else None
            await _process_share(
                share_name,
                explicit_pipeline_names,
                workspace_url,
                share_id,
                share_rec,
                share_id_to_pipelines,
                pipelines_list,
                pipeline_repo,
                share_repo,
                str(share_pack_id),
                pending_soft_deletes,
            )

        current_step = "Step 3/4: Deleting recipients"
        await tracker.update(current_step)
        for recipient_name in recipient_names:
            await _process_recipient(
                recipient_name,
                workspace_url,
                recipient_repo,
                str(share_pack_id),
                pending_soft_deletes,
            )

        current_step = "Step 4/4: Persisting DB soft-deletes"
        await tracker.update(current_step)
        db_ok, db_fail = await _persist_soft_deletes(pending_soft_deletes, str(share_pack_id))

        summary = _build_delete_summary(share_specs, recipient_names, db_ok, db_fail)
        await tracker.complete(summary)
        logger.success("Share pack {} DELETE strategy completed: {}", share_pack_id, summary)

    except Exception as e:
        await tracker.fail(str(e), current_step or "DELETE failed")
        logger.opt(exception=True).error("DELETE failed for {}: {}", share_pack_id, e)
        raise
