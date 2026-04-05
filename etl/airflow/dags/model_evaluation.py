from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parent
FEATURE_SQL_PATH = BASE_DIR / "enrollment_features.sql"
LEGACY_FEATURE_SQL_PATH = BASE_DIR / "enrollment_features_legacy_section_professor.sql"

LEAKY_SNAPSHOT_COLUMNS = [
    "seats_taken",
    "current_waitlist",
    "waitlist_size",
    "fill_ratio",
    "waitlist_ratio",
    "enrollment_status",
]


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


def resolve_feature_sql(conn) -> Path:
    columns_df = pd.read_sql(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'section_professor'
            """
        ),
        conn,
    )
    columns = set(columns_df["column_name"].astype(str).tolist())
    if "course_id" in columns:
        return FEATURE_SQL_PATH
    return LEGACY_FEATURE_SQL_PATH


def load_feature_frame() -> pd.DataFrame:
    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:
        sql_path = resolve_feature_sql(conn)
        query = sql_path.read_text()
        return pd.read_sql(text(query), conn)


def _parse_meeting_time(value: object) -> int | None:
    if value is None:
        return None
    value_str = str(value).strip()
    if not value_str or value_str.upper() == "TBA":
        return None
    parsed = pd.to_datetime(value_str, format="%I:%M%p", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(value_str, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.hour * 60 + parsed.minute


def engineer_python_features(df: pd.DataFrame) -> pd.DataFrame:
    features = df.copy()
    features["meetings_start_minutes"] = features["meetings_start_time"].map(_parse_meeting_time)
    features["meetings_end_minutes"] = features["meetings_end_time"].map(_parse_meeting_time)
    features["meeting_duration_minutes"] = features["meetings_end_minutes"] - features["meetings_start_minutes"]
    features["is_morning"] = features["meetings_start_minutes"].between(360, 719, inclusive="both")
    features["is_afternoon"] = features["meetings_start_minutes"].between(720, 1019, inclusive="both")
    features["is_evening"] = features["meetings_start_minutes"].ge(1020)

    day_letters = {
        "M": "meets_monday",
        "T": "meets_tuesday",
        "W": "meets_wednesday",
        "R": "meets_thursday",
        "F": "meets_friday",
        "S": "meets_saturday",
        "U": "meets_sunday",
    }
    days = features["meetings_days"].fillna("")
    for letter, column in day_letters.items():
        features[column] = days.str.contains(letter, regex=False)
    return features


def build_model_inputs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    feature_frame = engineer_python_features(df)
    drop_columns = [
        "filled_binary",
        "term_id",
        "course_id",
        "class_nbr",
        "title",
        "meetings_days",
        "meetings_start_time",
        "meetings_end_time",
        *LEAKY_SNAPSHOT_COLUMNS,
    ]
    x = feature_frame.drop(columns=drop_columns, errors="ignore")
    y = feature_frame["filled_binary"].astype(int)
    x = pd.get_dummies(
        x,
        columns=["season", "subject_id", "component", "course_level"],
        dummy_na=True,
    ).fillna(0)
    return x, y


def train_test_split_by_term(
    x: pd.DataFrame,
    y: pd.Series,
    raw_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, str, str]:
    ordered_terms = sorted(raw_df["term_id"].astype(str).unique().tolist())
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


def standardize_frames(x_train: pd.DataFrame, x_test: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    train = x_train.astype(float).to_numpy()
    test = x_test.astype(float).to_numpy()
    means = train.mean(axis=0)
    scales = train.std(axis=0)
    scales[scales == 0] = 1.0
    return (train - means) / scales, (test - means) / scales


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
    }


def evaluate_current_model() -> dict[str, object]:
    raw_df = load_feature_frame()
    x, y = build_model_inputs(raw_df)
    x_train, x_test, y_train, y_test, train_term, test_term = train_test_split_by_term(x, y, raw_df)
    train_scaled, test_scaled = standardize_frames(x_train, x_test)

    model = LogisticRegressionGD()
    model.fit(train_scaled, y_train.to_numpy(dtype=float))
    test_prob = model.predict_proba(test_scaled)
    train_prob = model.predict_proba(train_scaled)

    return {
        "rows": len(raw_df),
        "features": x.shape[1],
        "train_term": train_term,
        "test_term": test_term,
        "train_metrics": classification_metrics(y_train.to_numpy(dtype=int), train_prob),
        "test_metrics": classification_metrics(y_test.to_numpy(dtype=int), test_prob),
    }


if __name__ == "__main__":
    print(evaluate_current_model())
