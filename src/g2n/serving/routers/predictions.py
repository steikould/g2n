"""Real-time prediction endpoints for G2N models."""

from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException

router = APIRouter()


class DeductionClassifyRequest(BaseModel):
    """Request body for deduction classification."""

    remittance_text: str = Field(..., description="Raw remittance text to classify")


class DeductionClassifyResponse(BaseModel):
    """Response body for deduction classification."""

    deduction_type: str
    confidence: float


class ContractSimulationRequest(BaseModel):
    """Request body for contract simulation."""

    customer_segment: str
    tier_thresholds: list[float]
    discount_pcts: list[float]
    rebate_pcts: list[float]


class ContractSimulationResponse(BaseModel):
    """Response body for contract simulation."""

    projected_gross_revenue: float
    projected_net_revenue: float
    projected_g2n_spread: float
    net_price_change_pct: float


@router.post("/predict/deduction-classify", response_model=DeductionClassifyResponse)
async def classify_deduction(request: DeductionClassifyRequest) -> DeductionClassifyResponse:
    """Classify a deduction from remittance text in real time."""
    try:
        from g2n.models.deduction_classification.predict import predict_single

        result = predict_single(
            model_uri="models:/deduction_classification/Production",
            text=request.remittance_text,
        )
        return DeductionClassifyResponse(
            deduction_type=result["deduction_type"],
            confidence=result["confidence"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict/contract-simulate", response_model=ContractSimulationResponse)
async def simulate_contract(
    request: ContractSimulationRequest,
) -> ContractSimulationResponse:
    """Run a contract optimization simulation."""
    try:
        from g2n.models.contract_optimization.simulator import (
            ContractSimulator,
            SimulationScenario,
        )

        # In production, historical_sales would come from Snowflake
        raise HTTPException(
            status_code=501,
            detail="Contract simulation requires historical sales data connection.",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
