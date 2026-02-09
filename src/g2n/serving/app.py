"""FastAPI application entry point for G2N model serving on OpenShift."""

from __future__ import annotations

from fastapi import FastAPI

from g2n.serving.middleware.audit import AuditMiddleware
from g2n.serving.routers import features, health, predictions

app = FastAPI(
    title="G2N Model Serving API",
    description="Real-time inference and feature access for the G2N platform.",
    version="0.1.0",
)

app.add_middleware(AuditMiddleware)

app.include_router(health.router, tags=["health"])
app.include_router(predictions.router, prefix="/api/v1", tags=["predictions"])
app.include_router(features.router, prefix="/api/v1", tags=["features"])
