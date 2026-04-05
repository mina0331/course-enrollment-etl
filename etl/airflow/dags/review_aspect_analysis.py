from __future__ import annotations

import math
import re

from sqlalchemy import text

from course_forum_reviews import clean_text, sentiment_from_text


ASPECT_CONFIG = {
    "difficulty": {
        "keywords": [
            "difficult",
            "difficulty",
            "easy",
            "hard",
            "challenging",
            "brutal",
            "tough",
            "manageable",
        ],
    },
    "workload": {
        "keywords": [
            "workload",
            "assignment",
            "assignments",
            "homework",
            "problem set",
            "weekly",
            "busywork",
            "reading",
            "time consuming",
            "time-consuming",
        ],
    },
    "professor_personality": {
        "keywords": [
            "professor",
            "teacher",
            "instructor",
            "kind",
            "nice",
            "mean",
            "helpful",
            "caring",
            "rude",
            "funny",
            "engaging",
            "knowledgeable",
            "organized",
            "understanding",
        ],
    },
    "assessments": {
        "keywords": [
            "exam",
            "exams",
            "midterm",
            "midterms",
            "final",
            "quiz",
            "quizzes",
            "project",
            "projects",
            "paper",
            "papers",
            "presentation",
            "presentations",
        ],
    },
}

EXAM_TERMS = ["exam", "exams", "midterm", "midterms", "final", "quiz", "quizzes", "test", "tests"]
PROJECT_TERMS = ["project", "projects", "paper", "papers", "presentation", "presentations"]
ASSIGNMENT_TERMS = ["assignment", "assignments", "homework", "problem set", "problem sets"]
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+")
REVIEW_BATCH_SIZE = 250


def ensure_aspect_tables(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS course_forum_review_aspect (
                review_key TEXT NOT NULL,
                aspect_name TEXT NOT NULL,
                mention_count INTEGER NOT NULL,
                sentiment_score DOUBLE PRECISION,
                sentiment_label TEXT,
                exam_mentions INTEGER NOT NULL DEFAULT 0,
                project_mentions INTEGER NOT NULL DEFAULT 0,
                assignment_mentions INTEGER NOT NULL DEFAULT 0,
                supporting_text TEXT,
                PRIMARY KEY (review_key, aspect_name),
                FOREIGN KEY (review_key) REFERENCES course_forum_review(review_key)
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS course_forum_review_aspect_summary (
                course_id INTEGER NOT NULL,
                professor_id INTEGER,
                subject TEXT NOT NULL,
                catalog_nbr TEXT NOT NULL,
                professor_name TEXT NOT NULL,
                aspect_name TEXT NOT NULL,
                review_mentions INTEGER NOT NULL,
                total_mentions INTEGER NOT NULL,
                avg_sentiment_score DOUBLE PRECISION,
                positive_mentions INTEGER NOT NULL,
                neutral_mentions INTEGER NOT NULL,
                negative_mentions INTEGER NOT NULL,
                exam_mentions INTEGER NOT NULL DEFAULT 0,
                project_mentions INTEGER NOT NULL DEFAULT 0,
                assignment_mentions INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (course_id, professor_name, aspect_name)
            )
            """
        )
    )


def iter_reviews(conn, batch_size: int = REVIEW_BATCH_SIZE):
    offset = 0
    while True:
        rows = conn.execute(
            text(
                """
                SELECT review_key, course_id, professor_id, subject, catalog_nbr, professor_name, full_review_text
                FROM course_forum_review
                ORDER BY review_key
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": batch_size, "offset": offset},
        ).mappings().all()
        if not rows:
            break
        yield rows
        offset += len(rows)


def split_sentences(text_value: str) -> list[str]:
    cleaned = clean_text(text_value)
    if not cleaned:
        return []
    return [segment.strip() for segment in SENTENCE_SPLIT_PATTERN.split(cleaned) if segment.strip()]


def count_term_mentions(text_value: str, terms: list[str]) -> int:
    lowered = text_value.lower()
    return sum(lowered.count(term) for term in terms)


def label_from_score(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.20:
        return "positive"
    if score <= -0.20:
        return "negative"
    return "neutral"


def analyze_aspects_for_review(text_value: str) -> dict[str, dict[str, object]]:
    sentences = split_sentences(text_value)
    lowered_sentences = [sentence.lower() for sentence in sentences]
    aspect_results: dict[str, dict[str, object]] = {}

    for aspect_name, config in ASPECT_CONFIG.items():
        matched_sentences = [
            sentence
            for sentence, lowered in zip(sentences, lowered_sentences)
            if any(keyword in lowered for keyword in config["keywords"])
        ]
        mention_count = len(matched_sentences)
        supporting_text = " ".join(matched_sentences)
        score = sentiment_from_text(supporting_text) if supporting_text else None

        aspect_results[aspect_name] = {
            "mention_count": mention_count,
            "sentiment_score": score,
            "sentiment_label": label_from_score(score),
            "supporting_text": supporting_text or None,
            "exam_mentions": count_term_mentions(supporting_text, EXAM_TERMS),
            "project_mentions": count_term_mentions(supporting_text, PROJECT_TERMS),
            "assignment_mentions": count_term_mentions(supporting_text, ASSIGNMENT_TERMS),
        }

    return aspect_results


def flush_review_aspect_rows(conn, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO course_forum_review_aspect (
                review_key,
                aspect_name,
                mention_count,
                sentiment_score,
                sentiment_label,
                exam_mentions,
                project_mentions,
                assignment_mentions,
                supporting_text
            )
            VALUES (
                :review_key,
                :aspect_name,
                :mention_count,
                :sentiment_score,
                :sentiment_label,
                :exam_mentions,
                :project_mentions,
                :assignment_mentions,
                :supporting_text
            )
            """
        ),
        rows,
    )


def flush_summary_rows(conn, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO course_forum_review_aspect_summary (
                course_id,
                professor_id,
                subject,
                catalog_nbr,
                professor_name,
                aspect_name,
                review_mentions,
                total_mentions,
                avg_sentiment_score,
                positive_mentions,
                neutral_mentions,
                negative_mentions,
                exam_mentions,
                project_mentions,
                assignment_mentions
            )
            VALUES (
                :course_id,
                :professor_id,
                :subject,
                :catalog_nbr,
                :professor_name,
                :aspect_name,
                :review_mentions,
                :total_mentions,
                :avg_sentiment_score,
                :positive_mentions,
                :neutral_mentions,
                :negative_mentions,
                :exam_mentions,
                :project_mentions,
                :assignment_mentions
            )
            ON CONFLICT (course_id, professor_name, aspect_name) DO UPDATE SET
                professor_id = EXCLUDED.professor_id,
                subject = EXCLUDED.subject,
                catalog_nbr = EXCLUDED.catalog_nbr,
                review_mentions = EXCLUDED.review_mentions,
                total_mentions = EXCLUDED.total_mentions,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                positive_mentions = EXCLUDED.positive_mentions,
                neutral_mentions = EXCLUDED.neutral_mentions,
                negative_mentions = EXCLUDED.negative_mentions,
                exam_mentions = EXCLUDED.exam_mentions,
                project_mentions = EXCLUDED.project_mentions,
                assignment_mentions = EXCLUDED.assignment_mentions
            """
        ),
        rows,
    )


def run_review_aspect_analysis(conn) -> dict[str, int]:
    ensure_aspect_tables(conn)
    conn.execute(text("DELETE FROM course_forum_review_aspect"))
    conn.execute(text("DELETE FROM course_forum_review_aspect_summary"))

    review_aspect_rows: list[dict[str, object]] = []
    summary_accumulator: dict[tuple[object, ...], dict[str, object]] = {}
    processed_reviews = 0

    for review_batch in iter_reviews(conn):
        for review in review_batch:
            processed_reviews += 1
            aspect_results = analyze_aspects_for_review(str(review["full_review_text"]))
            for aspect_name, result in aspect_results.items():
                if result["mention_count"] == 0:
                    continue
                review_aspect_rows.append(
                    {
                        "review_key": review["review_key"],
                        "aspect_name": aspect_name,
                        "mention_count": result["mention_count"],
                        "sentiment_score": result["sentiment_score"],
                        "sentiment_label": result["sentiment_label"],
                        "exam_mentions": result["exam_mentions"],
                        "project_mentions": result["project_mentions"],
                        "assignment_mentions": result["assignment_mentions"],
                        "supporting_text": result["supporting_text"],
                    }
                )

                summary_key = (
                    review["course_id"],
                    review["professor_id"],
                    review["subject"],
                    review["catalog_nbr"],
                    review["professor_name"],
                    aspect_name,
                )
                summary = summary_accumulator.setdefault(
                    summary_key,
                    {
                        "course_id": review["course_id"],
                        "professor_id": review["professor_id"],
                        "subject": review["subject"],
                        "catalog_nbr": review["catalog_nbr"],
                        "professor_name": review["professor_name"],
                        "aspect_name": aspect_name,
                        "review_mentions": 0,
                        "total_mentions": 0,
                        "sentiment_total": 0.0,
                        "sentiment_count": 0,
                        "positive_mentions": 0,
                        "neutral_mentions": 0,
                        "negative_mentions": 0,
                        "exam_mentions": 0,
                        "project_mentions": 0,
                        "assignment_mentions": 0,
                    },
                )
                summary["review_mentions"] += 1
                summary["total_mentions"] += int(result["mention_count"])
                if result["sentiment_score"] is not None:
                    summary["sentiment_total"] += float(result["sentiment_score"])
                    summary["sentiment_count"] += 1
                if result["sentiment_label"] == "positive":
                    summary["positive_mentions"] += 1
                elif result["sentiment_label"] == "negative":
                    summary["negative_mentions"] += 1
                else:
                    summary["neutral_mentions"] += 1
                summary["exam_mentions"] += int(result["exam_mentions"])
                summary["project_mentions"] += int(result["project_mentions"])
                summary["assignment_mentions"] += int(result["assignment_mentions"])

        if len(review_aspect_rows) >= 500:
            flush_review_aspect_rows(conn, review_aspect_rows)
            review_aspect_rows.clear()

    if review_aspect_rows:
        flush_review_aspect_rows(conn, review_aspect_rows)

    summary_rows = []
    for summary in summary_accumulator.values():
        summary_rows.append(
            {
                "course_id": summary["course_id"],
                "professor_id": summary["professor_id"],
                "subject": summary["subject"],
                "catalog_nbr": summary["catalog_nbr"],
                "professor_name": summary["professor_name"],
                "aspect_name": summary["aspect_name"],
                "review_mentions": summary["review_mentions"],
                "total_mentions": summary["total_mentions"],
                "avg_sentiment_score": (
                    summary["sentiment_total"] / summary["sentiment_count"]
                    if summary["sentiment_count"]
                    else None
                ),
                "positive_mentions": summary["positive_mentions"],
                "neutral_mentions": summary["neutral_mentions"],
                "negative_mentions": summary["negative_mentions"],
                "exam_mentions": summary["exam_mentions"],
                "project_mentions": summary["project_mentions"],
                "assignment_mentions": summary["assignment_mentions"],
            }
        )

    flush_summary_rows(conn, summary_rows)
    return {
        "processed_reviews": processed_reviews,
        "summary_rows": len(summary_rows),
    }
