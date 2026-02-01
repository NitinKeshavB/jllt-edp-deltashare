"""
Smart Strategy Detection

Automatically detects optimal provisioning strategy (NEW vs UPDATE) by checking
if resources already exist in Databricks workspace.

This provides the best user experience by:
- Preventing "resource already exists" errors
- Automatically using UPDATE when resources exist
- Warning users when strategy is auto-corrected
"""

from datetime import datetime
from datetime import timezone
from typing import Dict
from typing import List

from databricks.sdk import WorkspaceClient
from loguru import logger

from dbrx_api.dbrx_auth.token_gen import get_auth_token


class StrategyDetectionResult:
    """Result of strategy detection analysis."""

    def __init__(self, user_strategy: str, detected_strategy: str):
        """
        Initialize strategy detection result.

        Args:
            user_strategy: Strategy specified by user in config
            detected_strategy: Strategy detected by analyzing workspace
        """
        self.user_strategy = user_strategy
        self.detected_strategy = detected_strategy
        self.strategy_changed = user_strategy != detected_strategy
        self.existing_recipients: List[str] = []
        self.existing_shares: List[str] = []
        self.new_recipients: List[str] = []
        self.new_shares: List[str] = []
        self.warnings: List[str] = []

    @property
    def final_strategy(self) -> str:
        """Return the strategy that should be used."""
        return self.detected_strategy

    def add_existing_recipient(self, name: str):
        """Record an existing recipient found in workspace."""
        self.existing_recipients.append(name)

    def add_existing_share(self, name: str):
        """Record an existing share found in workspace."""
        self.existing_shares.append(name)

    def add_new_recipient(self, name: str):
        """Record a new recipient not found in workspace."""
        self.new_recipients.append(name)

    def add_new_share(self, name: str):
        """Record a new share not found in workspace."""
        self.new_shares.append(name)

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)

    def get_summary(self) -> str:
        """Get human-readable summary of detection."""
        if not self.strategy_changed:
            return f"Strategy '{self.user_strategy}' confirmed - no conflicts detected"

        parts = [f"Strategy auto-changed: {self.user_strategy} → {self.detected_strategy}"]

        if self.existing_recipients:
            parts.append(
                f"Found {len(self.existing_recipients)} existing recipient(s): "
                f"{', '.join(self.existing_recipients[:3])}" + ("..." if len(self.existing_recipients) > 3 else "")
            )

        if self.existing_shares:
            parts.append(
                f"Found {len(self.existing_shares)} existing share(s): "
                f"{', '.join(self.existing_shares[:3])}" + ("..." if len(self.existing_shares) > 3 else "")
            )

        if self.new_recipients:
            parts.append(f"{len(self.new_recipients)} new recipient(s) will be created")

        if self.new_shares:
            parts.append(f"{len(self.new_shares)} new share(s) will be created")

        return ". ".join(parts)


async def detect_optimal_strategy(
    workspace_url: str, config: Dict, user_strategy: str, token_manager=None
) -> StrategyDetectionResult:
    """
    Detect optimal provisioning strategy by analyzing workspace.

    Logic:
    1. If user specifies UPDATE → Always use UPDATE (user knows best)
    2. If user specifies NEW:
       a. Check if any recipients/shares already exist
       b. If exists → Auto-switch to UPDATE
       c. If none exist → Keep NEW

    Args:
        workspace_url: Databricks workspace URL
        config: Parsed share pack configuration (dict)
        user_strategy: Strategy specified by user ("NEW" or "UPDATE")
        token_manager: Optional token manager for authentication

    Returns:
        StrategyDetectionResult with final strategy and analysis
    """
    result = StrategyDetectionResult(user_strategy, user_strategy)

    # If user explicitly wants UPDATE, respect that
    if user_strategy == "UPDATE":
        logger.info("User specified UPDATE strategy - no auto-detection needed")
        return result

    try:
        # Get Databricks client
        if token_manager:
            # Use injected token manager (from app.state)
            session_token = token_manager.get_token()
        else:
            # Fallback to direct auth
            session_token = get_auth_token(datetime.now(timezone.utc))[0]

        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        # Get all existing recipients
        logger.info("Checking existing recipients in workspace...")
        existing_recipients = {}
        try:
            for recipient in w_client.recipients.list():
                existing_recipients[recipient.name] = recipient
            logger.debug(f"Found {len(existing_recipients)} existing recipients")
        except Exception as e:
            logger.warning(f"Could not list recipients: {e}")
            # Continue with empty set - maybe user doesn't have permissions

        # Get all existing shares
        logger.info("Checking existing shares in workspace...")
        existing_shares = {}
        try:
            for share in w_client.shares.list_shares():
                existing_shares[share.name] = share
            logger.debug(f"Found {len(existing_shares)} existing shares")
        except Exception as e:
            logger.warning(f"Could not list shares: {e}")
            # Continue with empty set

        # Analyze recipients from config
        has_existing_recipient = False
        for recipient_config in config.get("recipient", []):
            recipient_name = recipient_config["name"]

            if recipient_name in existing_recipients:
                result.add_existing_recipient(recipient_name)
                has_existing_recipient = True
                logger.debug(f"Recipient '{recipient_name}' already exists")
            else:
                result.add_new_recipient(recipient_name)
                logger.debug(f"Recipient '{recipient_name}' is new")

        # Analyze shares from config
        has_existing_share = False
        for share_config in config.get("share", []):
            share_name = share_config["name"]

            if share_name in existing_shares:
                result.add_existing_share(share_name)
                has_existing_share = True
                logger.debug(f"Share '{share_name}' already exists")
            else:
                result.add_new_share(share_name)
                logger.debug(f"Share '{share_name}' is new")

        # Determine strategy
        if has_existing_recipient or has_existing_share:
            # Some resources exist → Switch to UPDATE
            result.detected_strategy = "UPDATE"

            warning_msg = (
                f"Auto-switched from NEW to UPDATE: "
                f"{len(result.existing_recipients)} recipient(s) and "
                f"{len(result.existing_shares)} share(s) already exist. "
                f"Existing resources will be updated, new ones will be created."
            )
            result.add_warning(warning_msg)

            logger.warning(warning_msg)
            logger.info(
                f"Existing recipients: {result.existing_recipients[:5]}"
                + ("..." if len(result.existing_recipients) > 5 else "")
            )
            logger.info(
                f"Existing shares: {result.existing_shares[:5]}" + ("..." if len(result.existing_shares) > 5 else "")
            )

        else:
            # No resources exist → Keep NEW strategy
            logger.info("No existing resources found - keeping NEW strategy")

    except Exception as e:
        # If detection fails, use user's original strategy as fallback
        logger.error(f"Strategy detection failed: {e}", exc_info=True)
        result.add_warning(
            f"Could not auto-detect strategy (error: {str(e)}). "
            f"Using user-specified strategy '{user_strategy}'. "
            f"If provisioning fails, try switching to UPDATE strategy."
        )

    return result


async def validate_strategy_feasibility(
    workspace_url: str, config: Dict, strategy: str, token_manager=None
) -> tuple[bool, List[str]]:
    """
    Validate that the chosen strategy is feasible.

    For NEW strategy: Warn if resources exist (but allow it)
    For UPDATE strategy: Verify at least some resources exist

    Args:
        workspace_url: Databricks workspace URL
        config: Parsed share pack configuration
        strategy: Strategy to validate
        token_manager: Optional token manager

    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    warnings = []

    try:
        # Get Databricks client
        if token_manager:
            session_token = token_manager.get_token()
        else:
            session_token = get_auth_token(datetime.now(timezone.utc))[0]

        w_client = WorkspaceClient(host=workspace_url, token=session_token)

        # Get existing resources
        try:
            existing_recipients = {r.name for r in w_client.recipients.list()}
            existing_shares = {s.name for s in w_client.shares.list_shares()}
        except Exception as e:
            logger.warning(f"Could not list workspace resources: {e}")
            return True, [f"Warning: Could not verify workspace resources: {str(e)}"]

        # Check recipients
        config_recipients = {r["name"] for r in config.get("recipient", [])}
        config_shares = {s["name"] for s in config.get("share", [])}

        if strategy == "UPDATE":
            # For UPDATE, at least some resources should exist
            has_existing = bool((config_recipients & existing_recipients) or (config_shares & existing_shares))

            if not has_existing:
                warnings.append(
                    "UPDATE strategy specified but no existing resources found. "
                    "All resources will be created as new. Consider using NEW strategy."
                )

        elif strategy == "NEW":
            # For NEW, warn if resources exist (but don't block)
            existing_recipient_overlap = config_recipients & existing_recipients
            existing_share_overlap = config_shares & existing_shares

            if existing_recipient_overlap:
                warnings.append(
                    f"NEW strategy specified but {len(existing_recipient_overlap)} recipient(s) "
                    f"already exist: {', '.join(list(existing_recipient_overlap)[:3])}. "
                    f"Consider using UPDATE strategy or renaming resources."
                )

            if existing_share_overlap:
                warnings.append(
                    f"NEW strategy specified but {len(existing_share_overlap)} share(s) "
                    f"already exist: {', '.join(list(existing_share_overlap)[:3])}. "
                    f"Consider using UPDATE strategy or renaming resources."
                )

    except Exception as e:
        logger.error(f"Strategy validation failed: {e}", exc_info=True)
        warnings.append(f"Could not validate strategy feasibility: {str(e)}")

    # Always return True - warnings are informational
    return True, warnings
