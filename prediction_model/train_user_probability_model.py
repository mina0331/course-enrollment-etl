from __future__ import annotations

import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from prediction_model.student_training_data import build_student_training_data
from prediction_model.train_section_baseline import standardize_frames
from prediction_model.train_user_model import build_user_model_inputs, train_test_split_by_term


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -30, 30)
    return 1.0 / (1.0 + np.exp(-clipped))


def _logit(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, 1e-4, 1 - 1e-4)
    return np.log(clipped / (1.0 - clipped))


class RidgeProbabilityRegressor:
    def __init__(self, alpha: float = 25.0):
        self.alpha = alpha
        self.weights: np.ndarray | None = None

    def fit(self, x: np.ndarray, y_probability: np.ndarray) -> None:
        x_with_bias = np.c_[np.ones(len(x)), x]
        target = _logit(y_probability)

        regularizer = np.eye(x_with_bias.shape[1])
        regularizer[0, 0] = 0.0

        self.weights = np.linalg.solve(
            x_with_bias.T @ x_with_bias + self.alpha * regularizer,
            x_with_bias.T @ target,
        )

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        if self.weights is None:
            raise RuntimeError("Model has not been fit yet.")
        x_with_bias = np.c_[np.ones(len(x)), x]
        return _sigmoid(x_with_bias @ self.weights)


def probability_metrics(
    y_true_probability: np.ndarray,
    predicted_probability: np.ndarray,
    observed_binary: np.ndarray,
) -> dict[str, float]:
    mae = float(np.mean(np.abs(y_true_probability - predicted_probability)))
    rmse = float(np.sqrt(np.mean((y_true_probability - predicted_probability) ** 2)))
    brier_vs_binary = float(np.mean((predicted_probability - observed_binary) ** 2))
    correlation = float(np.corrcoef(y_true_probability, predicted_probability)[0, 1])

    return {
        "mae_probability": mae,
        "rmse_probability": rmse,
        "brier_against_binary_outcome": brier_vs_binary,
        "probability_correlation": correlation,
    }


def top_probability_weights(model: RidgeProbabilityRegressor, columns: list[str], limit: int = 12) -> pd.DataFrame:
    if model.weights is None:
        raise RuntimeError("Model has not been fit yet.")

    weights = pd.DataFrame(
        {
            "feature": ["bias", *columns],
            "weight": model.weights,
        }
    )
    weights["abs_weight"] = weights["weight"].abs()
    return weights.sort_values("abs_weight", ascending=False).head(limit)[["feature", "weight"]]


def train_and_evaluate_user_probability_model(
    database_url: str | None = None,
    *,
    n_students: int = 2000,
    seed: int = 42,
    alpha: float = 25.0,
) -> dict[str, object]:
    if database_url is not None:
        os.environ["DATABASE_URL"] = database_url

    raw_df = build_student_training_data(
        n_students=n_students,
        seed=seed,
    )
    x, y_binary = build_user_model_inputs(raw_df)
    y_probability = raw_df["got_in_probability"].astype(float)

    x_train, x_test, y_train_binary, y_test_binary, train_term, test_term = train_test_split_by_term(
        x,
        y_binary,
        raw_df,
    )
    y_train_probability = y_probability.loc[raw_df["term_id"].astype(str) == train_term].reset_index(drop=True)
    y_test_probability = y_probability.loc[raw_df["term_id"].astype(str) == test_term].reset_index(drop=True)

    standardized = standardize_frames(x_train, x_test)
    model = RidgeProbabilityRegressor(alpha=alpha)
    model.fit(standardized.train, y_train_probability.to_numpy(dtype=float))

    train_pred = model.predict_proba(standardized.train)
    test_pred = model.predict_proba(standardized.test)

    predictions = raw_df.loc[raw_df["term_id"].astype(str) == test_term, [
        "term_id",
        "student_id",
        "class_standing",
        "subject_id",
        "catalog_nbr",
        "class_nbr",
        "interest_area",
        "got_in",
        "got_in_probability",
    ]].copy()
    predictions["predicted_enrollment_probability"] = test_pred
    predictions["probability_error"] = (
        predictions["predicted_enrollment_probability"] - predictions["got_in_probability"]
    ).abs()
    predictions = predictions.sort_values(["student_id", "class_nbr"])

    return {
        "rows": len(raw_df),
        "features": x.shape[1],
        "train_term": train_term,
        "test_term": test_term,
        "alpha": alpha,
        "train_probability_metrics": probability_metrics(
            y_train_probability.to_numpy(dtype=float),
            train_pred,
            y_train_binary.to_numpy(dtype=float),
        ),
        "test_probability_metrics": probability_metrics(
            y_test_probability.to_numpy(dtype=float),
            test_pred,
            y_test_binary.to_numpy(dtype=float),
        ),
        "top_weights": top_probability_weights(model, x_train.columns.tolist()),
        "test_predictions": predictions,
    }


if __name__ == "__main__":
    results = train_and_evaluate_user_probability_model()

    print(f"Train term: {results['train_term']}")
    print(f"Test term: {results['test_term']}")
    print(f"Alpha: {results['alpha']}")
    print()
    print("Train probability metrics:")
    print(pd.Series(results["train_probability_metrics"]).to_string())
    print()
    print("Test probability metrics:")
    print(pd.Series(results["test_probability_metrics"]).to_string())
    print()
    print("Top weights:")
    print(results["top_weights"].to_string(index=False))
    print()
    print("Sample test predictions:")
    print(results["test_predictions"].head(15).to_string(index=False))
