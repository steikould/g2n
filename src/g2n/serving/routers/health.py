"""Health and readiness probe endpoints for OpenShift."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
async def health_check() -> dict[str, str]:
    """Liveness probe — confirms the service is running."""
    return {"status": "healthy"}


@router.get("/readyz")
async def readiness_check() -> dict[str, str]:
    """Readiness probe — confirms models are loaded and service can accept requests."""
    # TODO: Add actual model loading check
    return {"status": "ready"}
