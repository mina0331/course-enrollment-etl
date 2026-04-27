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
from prediction_model.train_section_baseline import (
    LogisticRegressionGD,
    classification_metrics,
    standardize_frames,
    top_feature_weights,
)


def build_user_model_inputs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    feature_frame = df.copy()

    drop_columns = [
        "got_in",
        "waitlisted",
        "got_in_probability",
        "student_id",
        "term_id",
        "course_id",
        "class_nbr",
        "title",
        "meetings_days",
        "meetings_start_time",
        "meetings_end_time",
        # Drop section snapshot/leakage fields for a more realistic user model.
        "enrollment_status",
        "seats_taken",
        "waitlist_size",
        "current_waitlist",
        "fill_ratio",
        "waitlist_ratio",
        "seat_pressure",
        "filled_binary",
        # Do not let the model cheat by seeing the exact noise terms used to
        # generate the synthetic outcome.
        "planning_noise",
        "term_shock",
        "course_shock",
        "subject_shock",
    ]

    x = feature_frame.drop(columns=drop_columns, errors="ignore")
    y = feature_frame["got_in"].astype(int)

    categorical_columns = [
        column
        for column in [
            "class_standing",
            "interest_area",
            "season",
            "subject_id",
            "component",
            "course_level",
        ]
        if column in x.columns
    ]

    x = pd.get_dummies(x, columns=categorical_columns, dummy_na=True).fillna(0)
    return x, y


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


def select_threshold_from_train(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    candidate_thresholds = [i / 100 for i in range(30, 71)]
    best_threshold = 0.50
    best_score = (-1.0, -1.0)

    for threshold in candidate_thresholds:
        metrics = classification_metrics(y_true, y_prob, threshold=threshold)
        score = (metrics["accuracy"], metrics["f1"])
        if score > best_score:
            best_score = score
            best_threshold = threshold

    return best_threshold


def train_and_evaluate_user_model(
    database_url: str | None = None,
    *,
    n_students: int = 2000,
    seed: int = 42,
) -> dict[str, object]:
    if database_url is not None:
        os.environ["DATABASE_URL"] = database_url

    raw_df = build_student_training_data(
        n_students=n_students,
        seed=seed,
    )
    x, y = build_user_model_inputs(raw_df)
    x_train, x_test, y_train, y_test, train_term, test_term = train_test_split_by_term(x, y, raw_df)

    standardized = standardize_frames(x_train, x_test)
    model = LogisticRegressionGD(learning_rate=0.05, epochs=4000, l2=1e-4)
    model.fit(standardized.train, y_train.to_numpy(dtype=float))

    train_prob = model.predict_proba(standardized.train)
    test_prob = model.predict_proba(standardized.test)
    threshold = select_threshold_from_train(y_train.to_numpy(dtype=int), train_prob)

    train_metrics = classification_metrics(
        y_train.to_numpy(dtype=int),
        train_prob,
        threshold=threshold,
    )
    test_metrics = classification_metrics(
        y_test.to_numpy(dtype=int),
        test_prob,
        threshold=threshold,
    )

    predictions = raw_df.loc[raw_df["term_id"].astype(str) == test_term, [
        "term_id",
        "student_id",
        "class_standing",
        "subject_id",
        "catalog_nbr",
        "class_nbr",
        "interest_area",
        "got_in",
    ]].copy()
    predictions["predicted_get_in_probability"] = test_prob
    predictions["predicted_get_in"] = (test_prob >= threshold).astype(int)
    predictions = predictions.sort_values(["student_id", "class_nbr"])

    return {
        "rows": len(raw_df),
        "features": x.shape[1],
        "train_term": train_term,
        "test_term": test_term,
        "classification_threshold": threshold,
        "train_metrics": train_metrics,
        "test_metrics": test_metrics,
        "top_weights": top_feature_weights(model, x_train.columns.tolist()),
        "test_predictions": predictions,
    }


if __name__ == "__main__":
    results = train_and_evaluate_user_model()

    print(f"Train term: {results['train_term']}")
    print(f"Test term: {results['test_term']}")
    print(f"Classification threshold: {results['classification_threshold']:.2f}")
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
    print("Sample test predictions:")
    print(results["test_predictions"].head(15).to_string(index=False))
