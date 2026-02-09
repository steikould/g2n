"""Synchronize DQ rules and results with Collibra governance platform.

Pulls active DQ rule sets from Collibra at runtime and pushes check results
back for governance tracking.
"""

from __future__ import annotations

from typing import Any

from g2n.common.connectors.collibra import CollibraClient
from g2n.common.logging import get_logger

logger = get_logger(__name__)


def pull_active_rules(domain_id: str) -> list[dict[str, Any]]:
    """Pull the active DQ rule set for a domain from Collibra.

    Args:
        domain_id: Collibra domain ID for the G2N entity.

    Returns:
        List of rule definitions with IDs, thresholds, and check types.
    """
    client = CollibraClient()
    try:
        rules = client.get_dq_rules(domain_id)
        logger.info("dq_rules_pulled", domain_id=domain_id, rule_count=len(rules))
        return rules
    finally:
        client.close()


def push_check_results(
    results: list[dict[str, Any]],
) -> None:
    """Push batch DQ check results to Collibra.

    Args:
        results: List of dicts with keys: rule_id, passed, details.
    """
    client = CollibraClient()
    try:
        for result in results:
            client.push_dq_results(
                rule_id=result["rule_id"],
                passed=result["passed"],
                result_details=result.get("details"),
            )
        logger.info("dq_results_pushed", count=len(results))
    finally:
        client.close()
