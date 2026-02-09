"""Deduction Auto-Classification training pipeline.

NLP/classification models to auto-categorize incoming deductions
from distributor remittance data and route for resolution.
"""

from __future__ import annotations

from typing import Any

import mlflow
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline

from g2n.models.base import BaseG2NModel


class DeductionClassificationModel(BaseG2NModel):
    """TF-IDF + Logistic Regression model for deduction text classification."""

    @property
    def use_case(self) -> str:
        return "deduction_classification"

    def train(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
    ) -> dict[str, float]:
        default_params = {
            "tfidf__max_features": 5000,
            "tfidf__ngram_range": (1, 2),
            "clf__C": 1.0,
            "clf__max_iter": 500,
        }
        if params:
            default_params.update(params)

        text_col = "remittance_text"
        target_col = "deduction_type"

        X = train_df[text_col]
        y = train_df[target_col]

        self._model = Pipeline([
            ("tfidf", TfidfVectorizer(
                max_features=default_params["tfidf__max_features"],
                ngram_range=default_params["tfidf__ngram_range"],
            )),
            ("clf", LogisticRegression(
                C=default_params["clf__C"],
                max_iter=default_params["clf__max_iter"],
                multi_class="multinomial",
            )),
        ])
        self._model.fit(X, y)

        cv_scores = cross_val_score(self._model, X, y, cv=5, scoring="f1_macro")

        mlflow.sklearn.log_model(self._model, artifact_path="model")

        return {
            "cv_f1_macro_mean": cv_scores.mean(),
            "cv_f1_macro_std": cv_scores.std(),
            "num_classes": len(y.unique()),
        }

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")

        result = df.copy()
        result["predicted_deduction_type"] = self._model.predict(df["remittance_text"])
        result["prediction_confidence"] = self._model.predict_proba(
            df["remittance_text"]
        ).max(axis=1)
        return result

    def evaluate(
        self,
        test_df: pd.DataFrame,
        predictions: pd.DataFrame,
    ) -> dict[str, float]:
        from sklearn.metrics import accuracy_score, f1_score

        y_true = test_df["deduction_type"]
        y_pred = predictions["predicted_deduction_type"]

        return {
            "accuracy": accuracy_score(y_true, y_pred),
            "f1_macro": f1_score(y_true, y_pred, average="macro"),
            "f1_weighted": f1_score(y_true, y_pred, average="weighted"),
        }
