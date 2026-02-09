"""Abstract base model with MLflow integration, audit trail, and model card generation.

All G2N models inherit from this base to ensure consistent experiment tracking,
governance, and reproducibility.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import mlflow
import pandas as pd

from g2n.common.config import get_settings
from g2n.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ModelCard:
    """Model documentation for governance and audit."""

    name: str
    version: str
    description: str
    use_case: str
    training_dataset: str
    feature_versions: dict[str, str]
    metrics: dict[str, float]
    parameters: dict[str, Any]
    limitations: str = ""
    ethical_considerations: str = ""


class BaseG2NModel(ABC):
    """Abstract base class for all G2N ML models."""

    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        self._model: Any = None
        self._run_id: str | None = None
        self._version: str | None = None

        settings = get_settings().mlflow
        mlflow.set_tracking_uri(settings.tracking_uri)
        mlflow.set_experiment(settings.experiment_name)

    @property
    @abstractmethod
    def use_case(self) -> str:
        """G2N use case name (e.g. 'accrual_forecast')."""

    @abstractmethod
    def train(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
    ) -> dict[str, float]:
        """Train the model and return evaluation metrics.

        Args:
            train_df: Training DataFrame with features and target.
            params: Model hyperparameters.
            sample_weights: Optional quality-based sample weights.

        Returns:
            Dict of metric name → value.
        """

    @abstractmethod
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate predictions.

        Args:
            df: Input DataFrame with features.

        Returns:
            DataFrame with predictions appended.
        """

    @abstractmethod
    def evaluate(
        self,
        test_df: pd.DataFrame,
        predictions: pd.DataFrame,
    ) -> dict[str, float]:
        """Evaluate model performance on test data.

        Returns:
            Dict of metric name → value.
        """

    def train_with_tracking(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
        tags: dict[str, str] | None = None,
    ) -> dict[str, float]:
        """Train with full MLflow tracking."""
        with mlflow.start_run(run_name=f"{self.model_name}_train") as run:
            self._run_id = run.info.run_id

            if params:
                mlflow.log_params(params)
            if tags:
                mlflow.set_tags(tags)

            mlflow.set_tag("g2n.use_case", self.use_case)
            mlflow.set_tag("g2n.model_name", self.model_name)

            metrics = self.train(train_df, params=params, sample_weights=sample_weights)

            mlflow.log_metrics(metrics)
            logger.info(
                "model_training_complete",
                model=self.model_name,
                run_id=self._run_id,
                metrics=metrics,
            )
            return metrics

    def generate_model_card(
        self,
        *,
        description: str,
        training_dataset: str,
        feature_versions: dict[str, str],
        metrics: dict[str, float],
        parameters: dict[str, Any],
    ) -> ModelCard:
        """Generate a model card for governance documentation."""
        return ModelCard(
            name=self.model_name,
            version=self._version or "0.0.0",
            description=description,
            use_case=self.use_case,
            training_dataset=training_dataset,
            feature_versions=feature_versions,
            metrics=metrics,
            parameters=parameters,
        )
