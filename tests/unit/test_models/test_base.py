"""Tests for base model class."""

from __future__ import annotations

from g2n.models.base import ModelCard


class TestModelCard:
    def test_model_card_creation(self):
        """ModelCard can be created with all fields."""
        card = ModelCard(
            name="test_model",
            version="1.0.0",
            description="A test model",
            use_case="accrual_forecast",
            training_dataset="s3a://g2n-data/features/training_v1",
            feature_versions={"purchase_velocity": "1.0", "contract_complexity": "2.0"},
            metrics={"mae": 0.05, "r2": 0.92},
            parameters={"n_estimators": 200},
        )
        assert card.name == "test_model"
        assert card.use_case == "accrual_forecast"
        assert len(card.feature_versions) == 2
        assert card.limitations == ""
