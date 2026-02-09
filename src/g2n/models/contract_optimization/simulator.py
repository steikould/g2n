"""Contract Optimization simulation engine.

What-if analysis on rebate structures, tier thresholds, and discount strategies
by customer segment. Designed for the Angular commercial application.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from g2n.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SimulationScenario:
    """A contract simulation scenario."""

    name: str
    customer_segment: str
    tier_thresholds: list[float]
    discount_pcts: list[float]
    rebate_pcts: list[float]
    effective_months: int = 12


@dataclass
class SimulationResult:
    """Output of a contract simulation."""

    scenario_name: str
    projected_gross_revenue: float
    projected_net_revenue: float
    projected_g2n_spread: float
    net_price_change_pct: float
    details: dict[str, Any]


class ContractSimulator:
    """Simulation engine for contract what-if analysis."""

    def __init__(
        self,
        historical_sales: pd.DataFrame,
        customer_elasticity: pd.DataFrame | None = None,
    ) -> None:
        """Initialize simulator with historical data.

        Args:
            historical_sales: Sales history with customer_id, product_id, amount columns.
            customer_elasticity: Optional elasticity estimates per customer segment.
        """
        self.historical_sales = historical_sales
        self.customer_elasticity = customer_elasticity

    def simulate(self, scenario: SimulationScenario) -> SimulationResult:
        """Run a single simulation scenario.

        Args:
            scenario: The contract scenario to simulate.

        Returns:
            SimulationResult with projected revenue impact.
        """
        segment_sales = self.historical_sales[
            self.historical_sales["customer_segment"] == scenario.customer_segment
        ]

        gross_revenue = segment_sales["gross_amount"].sum()

        # Estimate deductions under new tier/discount structure
        total_discount = 0.0
        for threshold, discount in zip(scenario.tier_thresholds, scenario.discount_pcts):
            qualifying = segment_sales[segment_sales["gross_amount"] >= threshold]
            total_discount += qualifying["gross_amount"].sum() * discount

        total_rebate = 0.0
        for threshold, rebate in zip(scenario.tier_thresholds, scenario.rebate_pcts):
            qualifying = segment_sales[segment_sales["gross_amount"] >= threshold]
            total_rebate += qualifying["gross_amount"].sum() * rebate

        net_revenue = gross_revenue - total_discount - total_rebate
        g2n_spread = gross_revenue - net_revenue

        baseline_net = gross_revenue * 0.85  # simplified baseline assumption
        net_price_change = (net_revenue - baseline_net) / baseline_net if baseline_net else 0

        result = SimulationResult(
            scenario_name=scenario.name,
            projected_gross_revenue=gross_revenue,
            projected_net_revenue=net_revenue,
            projected_g2n_spread=g2n_spread,
            net_price_change_pct=net_price_change,
            details={
                "total_discount": total_discount,
                "total_rebate": total_rebate,
                "segment": scenario.customer_segment,
            },
        )
        logger.info("simulation_complete", scenario=scenario.name)
        return result

    def compare_scenarios(
        self,
        scenarios: list[SimulationScenario],
    ) -> pd.DataFrame:
        """Run multiple scenarios and return comparison table."""
        results = [self.simulate(s) for s in scenarios]
        return pd.DataFrame([
            {
                "scenario": r.scenario_name,
                "gross_revenue": r.projected_gross_revenue,
                "net_revenue": r.projected_net_revenue,
                "g2n_spread": r.projected_g2n_spread,
                "net_price_change_pct": r.net_price_change_pct,
            }
            for r in results
        ])
