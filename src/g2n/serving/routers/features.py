"""Feature Access API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.get("/features")
async def list_features(query: str = "*") -> dict[str, list]:
    """Search the feature catalog."""
    try:
        from g2n.features.registry import FeatureRegistry

        registry = FeatureRegistry()
        try:
            results = registry.search(query)
            return {"features": results}
        finally:
            registry.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/{feature_name}")
async def get_feature_metadata(feature_name: str) -> dict:
    """Get metadata for a specific feature."""
    try:
        from g2n.features.registry import FeatureRegistry

        registry = FeatureRegistry()
        try:
            result = registry.get_by_name(feature_name)
            if result is None:
                raise HTTPException(status_code=404, detail=f"Feature '{feature_name}' not found")
            return result
        finally:
            registry.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
