from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parent
FEATURE_SQL_PATH = BASE_DIR / "enrollment_features.sql"
LEGACY_FEATURE_SQL_PATH = BASE_DIR / "enrollment_features_legacy_section_professor.sql"

# These are useful for in-semester monitoring, but they leak the outcome for a
# pre-registration prediction model.
LEAKY_SNAPSHOT_COLUMNS = [
    "seats_taken",
    "current_waitlist",
    "waitlist_size",
    "fill_ratio",
    "waitlist_ratio",
    "enrollment_status",
]


def load_feature_frame(
    database_url: str | None = None,
    sql_path: Path | None = None,
) -> pd.DataFrame:
    database_url = database_url or os.environ["DATABASE_URL"]
    engine = create_engine(database_url)
    with engine.begin() as conn:
        query = resolve_feature_sql(conn, sql_path=sql_path).read_text()
        return pd.read_sql(text(query), conn)


def resolve_feature_sql(conn, sql_path: Path | None = None) -> Path:
    if sql_path is not None:
        return sql_path

    dialect = conn.engine.dialect.name
    if dialect == "sqlite":
        columns_df = pd.read_sql(
            text("PRAGMA table_info(section_professor)"),
            conn,
        )
        columns = set(columns_df["name"].astype(str).tolist())
    else:
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
    features["meeting_duration_minutes"] = (
        features["meetings_end_minutes"] - features["meetings_start_minutes"]
    )

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


def build_model_inputs(
    df: pd.DataFrame,
    *,
    include_snapshot_features: bool = False,
) -> tuple[pd.DataFrame, pd.Series]:
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
    ]
    if not include_snapshot_features:
        drop_columns.extend(LEAKY_SNAPSHOT_COLUMNS)

    x = feature_frame.drop(columns=drop_columns, errors="ignore")
    y = feature_frame["filled_binary"].astype(int)

    x = pd.get_dummies(
        x,
        columns=["season", "subject_id", "component", "course_level"],
        dummy_na=True,
    )
    x = x.fillna(0)

    return x, y


def build_training_frame(
    database_url: str | None = None,
    *,
    include_snapshot_features: bool = False,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    raw_df = load_feature_frame(database_url=database_url)
    x, y = build_model_inputs(
        raw_df,
        include_snapshot_features=include_snapshot_features,
    )
    return x, y, raw_df


if __name__ == "__main__":
    x, y, raw_df = build_training_frame()
    print(f"Loaded {len(raw_df)} section rows")
    print(f"Feature matrix shape: {x.shape}")
    print(f"Target mean (filled_binary): {y.mean():.3f}")
    print(x.head())
