"""Evaluation for contract optimization scenarios."""

from __future__ import annotations

import pandas as pd


def evaluate_scenario_impact(
    comparison_df: pd.DataFrame,
    baseline_scenario: str,
) -> pd.DataFrame:
    """Compute relative impact of each scenario vs. baseline.

    Args:
        comparison_df: Output of ContractSimulator.compare_scenarios().
        baseline_scenario: Name of the baseline scenario row.

    Returns:
        DataFrame with delta columns relative to baseline.
    """
    baseline = comparison_df[comparison_df["scenario"] == baseline_scenario].iloc[0]

    result = comparison_df.copy()
    result["net_revenue_delta"] = result["net_revenue"] - baseline["net_revenue"]
    result["g2n_spread_delta"] = result["g2n_spread"] - baseline["g2n_spread"]
    result["net_revenue_pct_change"] = (
        result["net_revenue_delta"] / baseline["net_revenue"]
    ) if baseline["net_revenue"] != 0 else 0

    return result
