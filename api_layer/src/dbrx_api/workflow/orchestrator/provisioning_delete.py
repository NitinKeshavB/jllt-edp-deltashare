"""
Share Pack Provisioning - DELETE Strategy.

DELETE config is name-only: list of recipient names, list of share names.
Optionally per-share explicit pipeline names to unschedule/delete.

- If shares are mentioned and pipelines are NOT explicitly mentioned:
  unschedule (delete schedule only) all pipelines related to those shares.
- If pipelines are explicitly mentioned: add them to the list to unschedule
  and delete schedule (and delete the pipeline).

Order: schedules -> (explicit pipelines only) -> shares -> recipients.
"""

from typing import Any
from typing import Dict
from typing import List
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


def _recipient_names_from_config(config: Dict[str, Any]) -> List[str]:
    """Extract flat list of recipient names. Accepts list of strings or list of {name}."""
    raw = config.get("recipient") or []
    names = []
    for item in raw:
        if isinstance(item, str):
            if item.strip():
                names.append(item.strip())
        elif isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]).strip())
    return names


def _share_specs_from_config(config: Dict[str, Any]) -> List[Tuple[str, List[str]]]:
    """
    Extract (share_name, explicit_pipeline_name_prefixes) per share.
    Accepts share as list of strings (names only) or list of {name, pipelines?: [...]}.
    pipelines can be list of strings (name_prefix) or list of {name_prefix}.
    """
    raw = config.get("share") or []
    specs = []
    for item in raw:
        if isinstance(item, str):
            if item.strip():
                specs.append((item.strip(), []))
        elif isinstance(item, dict):
            name = item.get("name")
            if not name:
                continue
            share_name = str(name).strip()
            pipelines_raw = item.get("pipelines") or []
            pipeline_names = []
            for p in pipelines_raw:
                if isinstance(p, str) and p.strip():
                    pipeline_names.append(p.strip())
                elif isinstance(p, dict) and (p.get("name_prefix") or p.get("name")):
                    pipeline_names.append(str(p.get("name_prefix") or p.get("name")).strip())
            specs.append((share_name, pipeline_names))
    return specs


async def provision_sharepack_delete(pool, share_pack: Dict[str, Any]) -> None:
    """
    Provision a share pack using DELETE strategy (name-only config).

    Config: metadata.workspace_url, recipient: [names], share: [names or {name, pipelines?: [...]}].
    - Pipelines not mentioned: unschedule (delete schedule only) for all pipelines for that share.
    - Pipelines explicitly mentioned: unschedule and delete schedule, then delete the pipeline.

    Order: unschedule/delete schedules -> delete explicitly listed pipelines -> delete shares -> delete recipients.
    """
    import json

    share_pack_id = share_pack["share_pack_id"]
    tracker = StatusTracker(pool, share_pack_id)

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

    # Deferred soft-deletes: collect during Databricks ops, persist only after ALL succeed
    pending_soft_deletes: List[Tuple[Any, UUID, str]] = []  # (repo, entity_id, deletion_reason)

    # share_specs: [(share_name, explicit_pipeline_names), ...]

    current_step = ""

    try:
        current_step = "Step 1/4: Validating DELETE config"
        await tracker.update(current_step)
        share_pack_uuid = UUID(share_pack_id)
        shares_list = await share_repo.list_by_share_pack(share_pack_uuid)
        pipelines_list = await pipeline_repo.list_by_share_pack(share_pack_uuid)
        name_to_share = {rec["share_name"]: rec for rec in shares_list}
        share_id_to_pipelines: Dict[UUID, List[Dict[str, Any]]] = {}
        for rec in pipelines_list:
            sid = rec["share_id"]
            share_id_to_pipelines.setdefault(sid, []).append(rec)

        # Step 2: For each share – unschedule (and optionally delete) pipelines, then delete share
        current_step = "Step 2/4: Unscheduling pipelines and deleting shares"
        await tracker.update(current_step)

        for share_name, explicit_pipeline_names in share_specs:
            share_rec = name_to_share.get(share_name)
            share_id = share_rec["share_id"] if share_rec else None

            # Pipelines to process for this share
            if explicit_pipeline_names:
                # Explicit pipelines: unschedule + delete pipeline
                for name_prefix in explicit_pipeline_names:
                    try:
                        pipelines = list_pipelines_with_search_criteria(
                            dltshr_workspace_url=workspace_url,
                            filter_expr=name_prefix,
                        )
                        pipeline_id = None
                        for p in pipelines:
                            if p.name == name_prefix:
                                pipeline_id = p.pipeline_id
                                break
                        if pipeline_id:
                            sch_result = delete_schedule_for_pipeline(
                                dltshr_workspace_url=workspace_url,
                                pipeline_id=pipeline_id,
                            )
                            if isinstance(sch_result, str):
                                if "no schedules found" in sch_result.lower() or (
                                    "not found" in sch_result.lower() and "error" not in sch_result.lower()
                                ):
                                    logger.info(
                                        "Schedule for pipeline '%s' does not exist in Databricks, "
                                        "skipping schedule deletion",
                                        name_prefix,
                                    )
                                elif "error" in sch_result.lower():
                                    raise RuntimeError(
                                        f"Failed to delete schedule for pipeline {name_prefix}: {sch_result}"
                                    )
                            result = delete_pipeline(
                                dltshr_workspace_url=workspace_url,
                                pipeline_id=pipeline_id,
                            )
                            if result is not None:
                                raise RuntimeError(f"Failed to delete pipeline {name_prefix}: {result}")
                            logger.info(f"Deleted pipeline (explicit): {name_prefix}")
                            # Collect DB soft-delete for later
                            for rec in pipelines_list:
                                if rec.get("pipeline_name") == name_prefix:
                                    pending_soft_deletes.append(
                                        (
                                            pipeline_repo,
                                            rec["pipeline_id"],
                                            f"DELETE strategy: share pack {share_pack_id}",
                                        )
                                    )
                                    break
                        else:
                            logger.info(
                                "Pipeline '%s' does not exist in Databricks, skipping deletion",
                                name_prefix,
                            )
                            # Collect DB soft-delete for later (not found in Databricks)
                            for rec in pipelines_list:
                                if rec.get("pipeline_name") == name_prefix:
                                    pending_soft_deletes.append(
                                        (
                                            pipeline_repo,
                                            rec["pipeline_id"],
                                            f"DELETE strategy: share pack {share_pack_id} (not found in Databricks)",
                                        )
                                    )
                                    break
                    except Exception as e:
                        logger.error(f"Failed to delete pipeline {name_prefix}: {e}")
                        raise

            # Pipelines not explicitly mentioned: unschedule only (delete schedule, keep pipeline)
            if not explicit_pipeline_names and share_id and share_id in share_id_to_pipelines:
                for pipe_rec in share_id_to_pipelines[share_id]:
                    pipeline_name = pipe_rec.get("pipeline_name")
                    dbrx_pipeline_id = pipe_rec.get("databricks_pipeline_id")
                    if not dbrx_pipeline_id:
                        continue
                    try:
                        sch_result = delete_schedule_for_pipeline(
                            dltshr_workspace_url=workspace_url,
                            pipeline_id=dbrx_pipeline_id,
                        )
                        if isinstance(sch_result, str):
                            if "no schedules found" in sch_result.lower() or (
                                "not found" in sch_result.lower() and "error" not in sch_result.lower()
                            ):
                                logger.info(
                                    "Schedule for pipeline '%s' does not exist in Databricks, " "skipping unschedule",
                                    pipeline_name,
                                )
                            elif "error" in sch_result.lower():
                                raise RuntimeError(
                                    f"Failed to delete schedule for pipeline {pipeline_name}: {sch_result}"
                                )
                            else:
                                logger.info(f"Unscheduled pipeline: {pipeline_name}")
                        else:
                            logger.info(f"Unscheduled pipeline: {pipeline_name}")
                    except RuntimeError:
                        raise
                    except Exception as e:
                        logger.warning(f"Failed to unschedule {pipeline_name}: {e}")

            # Delete share in Databricks (collect soft-delete for later)
            existing_share = get_shares(share_name=share_name, dltshr_workspace_url=workspace_url)
            if existing_share is None:
                logger.info(f"Share '{share_name}' does not exist in Databricks, skipping deletion")
                # Collect DB soft-delete for later (not found in Databricks)
                share_records = await share_repo.list_by_share_name(share_name)
                for rec in share_records:
                    pending_soft_deletes.append(
                        (
                            share_repo,
                            rec["share_id"],
                            f"DELETE strategy: share pack {share_pack_id} (not found in Databricks)",
                        )
                    )
                continue
            try:
                result = delete_share(share_name=share_name, dltshr_workspace_url=workspace_url)
                if result is not None:
                    raise RuntimeError(f"Failed to delete share {share_name}: {result}")
                logger.info(f"Deleted share: {share_name}")
                # Collect DB soft-delete for later (across all share packs)
                share_records = [share_rec] if share_rec else await share_repo.list_by_share_name(share_name)
                for rec in share_records:
                    pending_soft_deletes.append(
                        (
                            share_repo,
                            rec["share_id"],
                            f"DELETE strategy: share pack {share_pack_id}",
                        )
                    )
            except Exception as e:
                logger.error(f"Failed to delete share {share_name}: {e}")
                raise

        # Step 3: Delete recipients in Databricks (collect soft-deletes for later)
        current_step = "Step 3/4: Deleting recipients"
        await tracker.update(current_step)
        for recipient_name in recipient_names:
            try:
                # Check if recipient exists in Databricks before deleting
                existing_recipient = get_recipients(recipient_name, workspace_url)
                if existing_recipient is None:
                    logger.info(f"Recipient '{recipient_name}' does not exist in Databricks, skipping deletion")
                    # Collect DB soft-delete for later (not found in Databricks)
                    recipients_list = await recipient_repo.list_by_recipient_name(recipient_name)
                    for rec in recipients_list:
                        pending_soft_deletes.append(
                            (
                                recipient_repo,
                                rec["recipient_id"],
                                f"DELETE strategy: share pack {share_pack_id} (not found in Databricks)",
                            )
                        )
                    continue

                result = delete_recipient(
                    recipient_name=recipient_name,
                    dltshr_workspace_url=workspace_url,
                )
                if isinstance(result, str):
                    raise RuntimeError(f"Failed to delete recipient {recipient_name}: {result}")
                logger.info(f"Deleted recipient: {recipient_name}")

                # Collect DB soft-delete for later (across all share packs)
                recipients_list = await recipient_repo.list_by_recipient_name(recipient_name)
                for rec in recipients_list:
                    pending_soft_deletes.append(
                        (
                            recipient_repo,
                            rec["recipient_id"],
                            f"DELETE strategy: share pack {share_pack_id}",
                        )
                    )
            except Exception as e:
                logger.error(f"Failed to delete recipient {recipient_name}: {e}")
                raise

        # Step 4: All Databricks operations succeeded — now persist DB soft-deletes
        current_step = "Step 4/4: Persisting DB soft-deletes"
        await tracker.update(current_step)
        if pending_soft_deletes:
            for repo, entity_id, reason in pending_soft_deletes:
                await repo.soft_delete(
                    entity_id,
                    deleted_by="orchestrator",
                    deletion_reason=reason,
                    request_source="share_pack",
                )
            logger.info(f"Soft-deleted {len(pending_soft_deletes)} DB record(s) for share pack {share_pack_id}")

        await tracker.complete()
        logger.success(f"Share pack {share_pack_id} DELETE strategy completed")

    except Exception as e:
        await tracker.fail(str(e), current_step or "DELETE failed")
        logger.error(f"DELETE failed for {share_pack_id}: {e}", exc_info=True)
        raise
