from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from prediction_model.baseline import load_feature_frame


OUTPUT_PATH = Path(__file__).resolve().parent / "student_section_training_data.csv"


STANDING_CONFIG = {
    "senior": {"priority": 4, "mean_slot_hours": 12, "std_slot_hours": 6},
    "junior": {"priority": 3, "mean_slot_hours": 30, "std_slot_hours": 8},
    "sophomore": {"priority": 2, "mean_slot_hours": 54, "std_slot_hours": 10},
    "first_year": {"priority": 1, "mean_slot_hours": 78, "std_slot_hours": 12},
}

INTEREST_AREAS = {
    "quantitative": {"subjects": {"APMA", "MATH", "STAT", "ECON", "CS", "DS"}},
    "engineering": {"subjects": {"ENGR", "CS", "ECE", "SYS", "MAE", "BME", "CE"}},
    "life_sciences": {"subjects": {"BIOL", "CHEM", "NEUR", "PSYC", "PHYS"}},
    "humanities": {"subjects": {"ENWR", "ENGL", "HIST", "PHIL", "RELG", "CLAS"}},
    "social_sciences": {"subjects": {"ECON", "PLCP", "SOC", "ANTH", "PSYC", "GSAS"}},
    "languages": {"subjects": {"SPAN", "FREN", "GERM", "ITAL", "ARAB", "CHIN"}},
}


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-value))


def _sample_student_standing(rng: np.random.Generator, n_students: int) -> np.ndarray:
    standings = np.array(list(STANDING_CONFIG.keys()))
    probabilities = np.array([0.24, 0.26, 0.25, 0.25])
    return rng.choice(standings, size=n_students, p=probabilities)


def _sample_interest_area(rng: np.random.Generator, n_students: int) -> np.ndarray:
    areas = np.array(list(INTEREST_AREAS.keys()))
    probabilities = np.array([0.18, 0.17, 0.16, 0.17, 0.18, 0.14])
    return rng.choice(areas, size=n_students, p=probabilities)


def _parse_course_level(catalog_nbr: object) -> int:
    try:
        catalog_value = int(catalog_nbr)
    except (TypeError, ValueError):
        return 0
    return (catalog_value // 1000) * 1000


def _prepare_section_pool(section_df: pd.DataFrame) -> pd.DataFrame:
    pool = section_df.copy()
    pool["avg_prof_rating"] = pool["avg_prof_rating"].fillna(pool["avg_prof_rating"].median())
    pool["avg_prof_difficulty"] = pool["avg_prof_difficulty"].fillna(
        pool["avg_prof_difficulty"].median()
    )
    pool["avg_course_forum_sentiment"] = pool["avg_course_forum_sentiment"].fillna(0.0)
    pool["avg_course_forum_demand"] = pool["avg_course_forum_demand"].fillna(0.0)
    pool["avg_course_forum_review_count"] = pool["avg_course_forum_review_count"].fillna(0.0)
    pool["total_course_forum_reviews"] = pool["total_course_forum_reviews"].fillna(0.0)
    pool["course_level"] = pool["course_level"].fillna(pool["catalog_nbr"].map(_parse_course_level))
    pool["historical_demand"] = pool["avg_fill_ratio_prior_course"].fillna(
        pool["avg_fill_ratio_prior_subject_level"]
    )
    pool["historical_demand"] = pool["historical_demand"].fillna(pool["fill_ratio"]).fillna(0.5)
    pool["fill_ratio"] = pool["fill_ratio"].fillna(pool["historical_demand"]).fillna(0.5)
    pool["waitlist_ratio"] = pool["waitlist_ratio"].fillna(0.0)
    pool["capacity"] = pool["capacity"].fillna(pool["capacity"].median()).fillna(25)
    pool["current_waitlist"] = pool["current_waitlist"].fillna(0.0)
    pool["course_forum_signal"] = (
        pool["avg_course_forum_demand"].clip(lower=0)
        + 0.15 * pool["avg_course_forum_sentiment"].clip(lower=-1, upper=1)
        + 0.03 * np.log1p(pool["total_course_forum_reviews"].clip(lower=0))
    )
    pool["seat_pressure"] = (
        pool["fill_ratio"].clip(lower=0, upper=1.5)
        + 0.1 * pool["current_waitlist"].clip(lower=0)
    )
    pool["is_morning_course"] = pool["meetings_start_time"].fillna("").astype(str).str.contains(
        "AM",
        regex=False,
    )
    return pool


def _subject_interest_boost(subject: object, interest_area: str) -> float:
    subject_str = str(subject).upper() if subject is not None else ""
    if subject_str in INTEREST_AREAS[interest_area]["subjects"]:
        return 1.0
    return 0.0


def generate_student_profiles(
    n_students: int = 2000,
    *,
    seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    student_ids = np.arange(1, n_students + 1)
    standings = _sample_student_standing(rng, n_students)
    interest_areas = _sample_interest_area(rng, n_students)

    records: list[dict[str, object]] = []
    for student_id, standing, interest_area in zip(student_ids, standings, interest_areas):
        config = STANDING_CONFIG[standing]
        enrollment_slot_hours = max(
            0.0,
            rng.normal(config["mean_slot_hours"], config["std_slot_hours"]),
        )
        max_course_level = {
            "first_year": 2000,
            "sophomore": 3000,
            "junior": 4000,
            "senior": 4000,
        }[standing]

        records.append(
            {
                "student_id": student_id,
                "class_standing": standing,
                "interest_area": interest_area,
                "priority_score": config["priority"],
                "enrollment_slot_hours": round(enrollment_slot_hours, 2),
                "max_course_level": max_course_level,
                "schedule_flexibility": round(float(rng.uniform(0.0, 1.0)), 3),
                "difficulty_tolerance": round(float(rng.normal(0.0, 1.0)), 3),
                "planning_noise": round(float(rng.normal(0.0, 0.65)), 3),
            }
        )

    return pd.DataFrame.from_records(records)


def simulate_student_section_attempts(
    section_df: pd.DataFrame,
    *,
    n_students: int = 2000,
    attempts_per_student: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    sections = _prepare_section_pool(section_df)
    students = generate_student_profiles(n_students=n_students, seed=seed)

    sampled_sections = sections.sample(
        n=n_students * attempts_per_student,
        replace=True,
        random_state=seed,
    ).reset_index(drop=True)
    repeated_students = students.loc[students.index.repeat(attempts_per_student)].reset_index(drop=True)

    training_df = pd.concat([repeated_students, sampled_sections], axis=1)
    training_df["attempt_id"] = np.arange(1, len(training_df) + 1)

    training_df["level_ok"] = (
        training_df["course_level"].fillna(0) <= training_df["max_course_level"]
    ).astype(int)
    training_df["interest_match"] = [
        _subject_interest_boost(subject, interest_area)
        for subject, interest_area in zip(training_df["subject_id"], training_df["interest_area"])
    ]
    training_df["time_penalty"] = (
        training_df["is_morning_course"].astype(int) * (1.0 - training_df["schedule_flexibility"])
    )
    training_df["difficulty_penalty"] = (
        (training_df["avg_prof_difficulty"].fillna(0) - 2.5) * (0.35 - training_df["difficulty_tolerance"])
    )
    training_df["term_shock"] = rng.normal(0.0, 0.30, size=len(training_df))
    training_df["course_shock"] = rng.normal(0.0, 0.22, size=len(training_df))
    training_df["registration_competitiveness"] = (
        training_df["historical_demand"].clip(0, 1.5)
        + training_df["fill_ratio"].clip(0, 1.5)
        + training_df["waitlist_ratio"].fillna(0).clip(0, 1.5)
        + 0.20 * training_df["course_forum_signal"].clip(lower=0, upper=3)
    )

    # The synthetic process is intentionally noisy so it can mimic future demand
    # uncertainty instead of replaying historical fill outcomes deterministically.
    linear_score = (
        0.9
        + 0.7 * training_df["priority_score"]
        - 0.035 * training_df["enrollment_slot_hours"]
        - 1.1 * training_df["registration_competitiveness"]
        - 0.02 * training_df["current_waitlist"].clip(lower=0)
        + 0.012 * training_df["capacity"].clip(lower=0, upper=500)
        + 0.40 * training_df["level_ok"]
        + 0.35 * training_df["interest_match"]
        - 0.45 * training_df["time_penalty"]
        - 0.22 * training_df["difficulty_penalty"]
        + training_df["planning_noise"]
        + training_df["term_shock"]
        + training_df["course_shock"]
    )

    probability = _sigmoid(np.nan_to_num(linear_score.to_numpy(), nan=0.0))
    probability = np.clip(probability, 0.001, 0.999)
    training_df["got_in_probability"] = probability
    training_df["got_in"] = rng.binomial(1, probability)
    training_df["waitlisted"] = (
        (training_df["got_in"] == 0) & (training_df["current_waitlist"] > 0)
    ).astype(int)

    return training_df


def build_student_training_data(
    *,
    n_students: int = 2000,
    attempts_per_student: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    section_features = load_feature_frame()
    return simulate_student_section_attempts(
        section_features,
        n_students=n_students,
        attempts_per_student=attempts_per_student,
        seed=seed,
    )


def export_student_training_data(
    output_path: Path = OUTPUT_PATH,
    *,
    n_students: int = 2000,
    attempts_per_student: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    training_df = build_student_training_data(
        n_students=n_students,
        attempts_per_student=attempts_per_student,
        seed=seed,
    )
    training_df.to_csv(output_path, index=False)
    return training_df


if __name__ == "__main__":
    df = export_student_training_data()
    print(f"Generated {len(df)} student-section rows")
    print(f"Saved synthetic training data to {OUTPUT_PATH}")
    print(
        df[
            [
                "student_id",
                "class_standing",
                "enrollment_slot_hours",
                "class_nbr",
                "subject_id",
                "catalog_nbr",
                "capacity",
                "fill_ratio",
                "current_waitlist",
                "historical_demand",
                "got_in_probability",
                "got_in",
                "waitlisted",
            ]
        ].head(10).to_string(index=False)
    )
