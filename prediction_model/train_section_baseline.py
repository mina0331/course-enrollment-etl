from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from prediction_model.baseline import build_training_frame


@dataclass
class StandardizedData:
    train: np.ndarray
    test: np.ndarray
    means: np.ndarray
    scales: np.ndarray


class LogisticRegressionGD:
    def __init__(self, learning_rate: float = 0.05, epochs: int = 4000, l2: float = 1e-4):
        self.learning_rate = learning_rate
        self.epochs = epochs
        self.l2 = l2
        self.weights: np.ndarray | None = None
        self.bias: float = 0.0

    @staticmethod
    def _sigmoid(values: np.ndarray) -> np.ndarray:
        clipped = np.clip(values, -30, 30)
        return 1.0 / (1.0 + np.exp(-clipped))

    def fit(self, x: np.ndarray, y: np.ndarray) -> None:
        n_samples, n_features = x.shape
        self.weights = np.zeros(n_features, dtype=float)
        self.bias = 0.0

        for _ in range(self.epochs):
            linear = x @ self.weights + self.bias
            preds = self._sigmoid(linear)
            errors = preds - y

            grad_w = (x.T @ errors) / n_samples + self.l2 * self.weights
            grad_b = errors.mean()

            self.weights -= self.learning_rate * grad_w
            self.bias -= self.learning_rate * grad_b

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        if self.weights is None:
            raise RuntimeError("Model has not been fit yet.")
        return self._sigmoid(x @ self.weights + self.bias)

    def predict(self, x: np.ndarray, threshold: float = 0.5) -> np.ndarray:
        return (self.predict_proba(x) >= threshold).astype(int)


def standardize_frames(x_train: pd.DataFrame, x_test: pd.DataFrame) -> StandardizedData:
    train = x_train.astype(float).to_numpy()
    test = x_test.astype(float).to_numpy()
    means = train.mean(axis=0)
    scales = train.std(axis=0)
    scales[scales == 0] = 1.0
    return StandardizedData(
        train=(train - means) / scales,
        test=(test - means) / scales,
        means=means,
        scales=scales,
    )


def train_test_split_by_term(
    x: pd.DataFrame,
    y: pd.Series,
    raw_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, str, str]:
    ordered_terms = sorted(raw_df["term_id"].astype(str).unique().tolist())
    if len(ordered_terms) < 2:
        raise ValueError("Need at least two terms for a term-based split.")

    train_term = ordered_terms[0]
    test_term = ordered_terms[-1]

    train_mask = raw_df["term_id"].astype(str) == train_term
    test_mask = raw_df["term_id"].astype(str) == test_term

    return (
        x.loc[train_mask].reset_index(drop=True),
        x.loc[test_mask].reset_index(drop=True),
        y.loc[train_mask].reset_index(drop=True),
        y.loc[test_mask].reset_index(drop=True),
        train_term,
        test_term,
    )


def roc_auc_score_manual(y_true: np.ndarray, y_score: np.ndarray) -> float:
    positives = y_true == 1
    negatives = y_true == 0
    n_pos = positives.sum()
    n_neg = negatives.sum()
    if n_pos == 0 or n_neg == 0:
        return float("nan")

    ranks = pd.Series(y_score).rank(method="average").to_numpy()
    rank_sum_pos = ranks[positives].sum()
    auc = (rank_sum_pos - (n_pos * (n_pos + 1) / 2.0)) / (n_pos * n_neg)
    return float(auc)


def classification_metrics(y_true: np.ndarray, y_prob: np.ndarray, threshold: float = 0.5) -> dict[str, float]:
    y_pred = (y_prob >= threshold).astype(int)

    tp = int(((y_pred == 1) & (y_true == 1)).sum())
    tn = int(((y_pred == 0) & (y_true == 0)).sum())
    fp = int(((y_pred == 1) & (y_true == 0)).sum())
    fn = int(((y_pred == 0) & (y_true == 1)).sum())

    accuracy = (tp + tn) / len(y_true) if len(y_true) else float("nan")
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    brier = float(np.mean((y_prob - y_true) ** 2))
    auc = roc_auc_score_manual(y_true, y_prob)

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "brier": brier,
        "roc_auc": auc,
        "tp": float(tp),
        "tn": float(tn),
        "fp": float(fp),
        "fn": float(fn),
    }


def top_feature_weights(model: LogisticRegressionGD, columns: list[str], limit: int = 12) -> pd.DataFrame:
    if model.weights is None:
        raise RuntimeError("Model has not been fit yet.")

    weights = pd.DataFrame({"feature": columns, "weight": model.weights})
    weights["abs_weight"] = weights["weight"].abs()
    return weights.sort_values("abs_weight", ascending=False).head(limit)[["feature", "weight"]]


def train_and_evaluate(database_url: str | None = None) -> dict[str, object]:
    if database_url is not None:
        os.environ["DATABASE_URL"] = database_url

    x, y, raw_df = build_training_frame(include_snapshot_features=False)
    x_train, x_test, y_train, y_test, train_term, test_term = train_test_split_by_term(x, y, raw_df)

    standardized = standardize_frames(x_train, x_test)
    model = LogisticRegressionGD(learning_rate=0.05, epochs=4000, l2=1e-4)
    model.fit(standardized.train, y_train.to_numpy(dtype=float))

    train_prob = model.predict_proba(standardized.train)
    test_prob = model.predict_proba(standardized.test)

    train_metrics = classification_metrics(y_train.to_numpy(dtype=int), train_prob)
    test_metrics = classification_metrics(y_test.to_numpy(dtype=int), test_prob)

    predictions = raw_df.loc[raw_df["term_id"].astype(str) == test_term, [
        "term_id",
        "subject_id",
        "catalog_nbr",
        "class_nbr",
        "filled_binary",
    ]].copy()
    predictions["predicted_fill_probability"] = test_prob
    predictions = predictions.sort_values("predicted_fill_probability", ascending=False)

    return {
        "train_term": train_term,
        "test_term": test_term,
        "train_metrics": train_metrics,
        "test_metrics": test_metrics,
        "top_weights": top_feature_weights(model, x_train.columns.tolist()),
        "test_predictions": predictions,
    }


if __name__ == "__main__":
    results = train_and_evaluate()

    print(f"Train term: {results['train_term']}")
    print(f"Test term: {results['test_term']}")
    print()
    print("Train metrics:")
    print(pd.Series(results["train_metrics"]).to_string())
    print()
    print("Test metrics:")
    print(pd.Series(results["test_metrics"]).to_string())
    print()
    print("Top feature weights:")
    print(results["top_weights"].to_string(index=False))
    print()
    print("Highest-risk predicted sections:")
    print(results["test_predictions"].head(15).to_string(index=False))
