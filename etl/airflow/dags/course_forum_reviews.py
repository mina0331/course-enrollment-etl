from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import math
import re
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from sqlalchemy import text


COURSE_FORUM_BASE_URL = "https://thecourseforum.com"
SEMESTER_PATTERN = re.compile(r"^(Spring|Summer|Fall|Winter)\s+\d{4}$", re.IGNORECASE)
UPDATED_PATTERN = re.compile(r"^Updated\s+\d{1,2}/\d{1,2}/\d{2}$", re.IGNORECASE)
NUMERIC_LINE_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")

# Lightweight lexicon sentiment keeps the dependency footprint small while still
# producing stable demand features when richer NLP packages are unavailable.
POSITIVE_WORDS = {
    "amazing",
    "awesome",
    "best",
    "clear",
    "cohesive",
    "easy",
    "effective",
    "engaging",
    "enjoyable",
    "excellent",
    "fabulous",
    "fair",
    "fantastic",
    "fun",
    "good",
    "great",
    "helpful",
    "interesting",
    "kind",
    "knowledgeable",
    "love",
    "manageable",
    "organized",
    "outstanding",
    "prepared",
    "recommend",
    "rewarding",
    "solid",
    "succeed",
    "understandable",
    "useful",
    "well",
    "wonderful",
}

NEGATIVE_WORDS = {
    "awful",
    "bad",
    "boring",
    "brutal",
    "busywork",
    "challenging",
    "confusing",
    "difficult",
    "disorganized",
    "frustrating",
    "hard",
    "harsh",
    "hate",
    "impossible",
    "inconsistent",
    "intimidating",
    "messy",
    "overwhelming",
    "poor",
    "rushed",
    "stressful",
    "tedious",
    "tough",
    "unclear",
    "unfair",
    "unhelpful",
    "useless",
    "worst",
}


@dataclass
class CourseProfessorLink:
    professor_name: str
    professor_page_url: str


@dataclass
class ParsedReview:
    review_term_label: str | None
    updated_at: str | None
    professor_review_text: str | None
    course_review_text: str | None
    full_review_text: str
    sentiment_score: float
    professor_sentiment_score: float | None
    course_sentiment_score: float | None
    sentiment_label: str


def ensure_course_forum_review_tables(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS course_forum_professor_page_raw_html (
                course_id INTEGER NOT NULL,
                subject TEXT NOT NULL,
                catalog_nbr TEXT NOT NULL,
                professor_name TEXT NOT NULL,
                professor_page_url TEXT NOT NULL PRIMARY KEY,
                raw_html TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS course_forum_review (
                review_key TEXT NOT NULL PRIMARY KEY,
                course_id INTEGER NOT NULL,
                subject TEXT NOT NULL,
                catalog_nbr TEXT NOT NULL,
                professor_id INTEGER,
                professor_name TEXT NOT NULL,
                professor_page_url TEXT NOT NULL,
                review_term_label TEXT,
                updated_at TEXT,
                professor_review_text TEXT,
                course_review_text TEXT,
                full_review_text TEXT NOT NULL,
                sentiment_score DOUBLE PRECISION NOT NULL,
                professor_sentiment_score DOUBLE PRECISION,
                course_sentiment_score DOUBLE PRECISION,
                sentiment_label TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS course_forum_review_summary (
                course_id INTEGER NOT NULL,
                professor_id INTEGER,
                subject TEXT NOT NULL,
                catalog_nbr TEXT NOT NULL,
                professor_name TEXT NOT NULL,
                professor_page_url TEXT NOT NULL,
                review_count INTEGER NOT NULL,
                positive_review_count INTEGER NOT NULL,
                neutral_review_count INTEGER NOT NULL,
                negative_review_count INTEGER NOT NULL,
                avg_sentiment_score DOUBLE PRECISION NOT NULL,
                avg_professor_sentiment_score DOUBLE PRECISION,
                avg_course_sentiment_score DOUBLE PRECISION,
                sentiment_demand_score DOUBLE PRECISION NOT NULL,
                most_recent_review_at TEXT,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (course_id, professor_name)
            )
            """
        )
    )


def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_professor_name(name: str) -> str:
    cleaned = clean_text(name)
    if "," in cleaned:
        last, first = [part.strip() for part in cleaned.split(",", 1)]
        if first and last:
            cleaned = f"{first} {last}"
    return cleaned


def tokenize(text_value: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", text_value.lower())


def sentiment_from_text(text_value: str | None) -> float | None:
    if not text_value:
        return None

    tokens = tokenize(text_value)
    if not tokens:
        return 0.0

    positive_hits = sum(token in POSITIVE_WORDS for token in tokens)
    negative_hits = sum(token in NEGATIVE_WORDS for token in tokens)
    magnitude = positive_hits + negative_hits

    if magnitude == 0:
        return 0.0

    polarity = (positive_hits - negative_hits) / magnitude
    confidence_scale = min(1.0, math.log1p(magnitude) / math.log(6))
    return max(-1.0, min(1.0, polarity * confidence_scale))


def sentiment_label(score: float | None) -> str:
    if score is None:
        return "unknown"
    if score >= 0.20:
        return "positive"
    if score <= -0.20:
        return "negative"
    return "neutral"


def extract_course_professor_links(raw_html: str, *, base_url: str = COURSE_FORUM_BASE_URL) -> list[CourseProfessorLink]:
    if not raw_html:
        return []

    soup = BeautifulSoup(raw_html, "html.parser")
    links: dict[str, CourseProfessorLink] = {}

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href"))
        if not href:
            continue
        if not re.search(r"/course/\d+/\d+/?$", href):
            continue

        professor_name = normalize_professor_name(anchor.get_text(" ", strip=True))
        if not professor_name:
            container = anchor.find_parent(["li", "div", "section", "article"])
            if container is not None:
                headings = container.find_all(["h1", "h2", "h3", "h4"])
                if headings:
                    professor_name = normalize_professor_name(headings[0].get_text(" ", strip=True))
        if not professor_name:
            continue

        absolute_url = urljoin(base_url, href)
        links[absolute_url] = CourseProfessorLink(
            professor_name=professor_name,
            professor_page_url=absolute_url,
        )

    return sorted(links.values(), key=lambda item: item.professor_name.lower())


def _candidate_review_blocks(soup: BeautifulSoup) -> list[str]:
    candidates: list[tuple[int, str]] = []
    seen = set()

    for tag in soup.find_all(["li", "div", "article", "section"]):
        lines = [clean_text(line) for line in tag.get_text("\n", strip=True).splitlines()]
        lines = [line for line in lines if line]
        if len(lines) < 4:
            continue
        if not any(UPDATED_PATTERN.match(line) for line in lines):
            continue
        if not any(SEMESTER_PATTERN.match(line) for line in lines):
            continue

        text_block = "\n".join(lines)
        if text_block in seen:
            continue
        seen.add(text_block)
        candidates.append((len(lines), text_block))

    candidates.sort(key=lambda item: item[0])
    return [block for _, block in candidates]


def _trim_review_metric_tail(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and (NUMERIC_LINE_PATTERN.match(trimmed[-1]) or trimmed[-1] in {"Show more", "Show less"}):
        trimmed.pop()
    return trimmed


def parse_course_professor_page(raw_html: str, professor_name: str) -> list[ParsedReview]:
    if not raw_html:
        return []

    soup = BeautifulSoup(raw_html, "html.parser")
    review_blocks = _candidate_review_blocks(soup)
    parsed_reviews: list[ParsedReview] = []
    seen_keys = set()

    for block in review_blocks:
        lines = [clean_text(line) for line in block.splitlines()]
        lines = [line for line in lines if line]
        lines = _trim_review_metric_tail(lines)
        if len(lines) < 3:
            continue

        review_term = next((line for line in lines if SEMESTER_PATTERN.match(line)), None)
        updated_at = next((line.replace("Updated", "", 1).strip() for line in lines if UPDATED_PATTERN.match(line)), None)

        try:
            start_index = lines.index(next(line for line in lines if UPDATED_PATTERN.match(line))) + 1
        except StopIteration:
            continue

        content_lines = [line for line in lines[start_index:] if line not in {professor_name, normalize_professor_name(professor_name)}]
        content_lines = [line for line in content_lines if not SEMESTER_PATTERN.match(line) and not UPDATED_PATTERN.match(line)]
        content_lines = [line for line in content_lines if line not in {"Add your review!", "Questions", "Ask your Question!", "No Questions and Answers"}]

        if not content_lines:
            continue

        professor_review_text = content_lines[0]
        course_review_text = "\n\n".join(content_lines[1:]) if len(content_lines) > 1 else None
        full_review_text = "\n\n".join(content_lines)
        full_review_text = clean_text(full_review_text.replace("\n\n", " "))
        if not full_review_text:
            continue

        professor_score = sentiment_from_text(professor_review_text)
        course_score = sentiment_from_text(course_review_text)
        overall_score = sentiment_from_text(full_review_text)
        overall_score = overall_score if overall_score is not None else 0.0

        review_identity = (review_term or "", updated_at or "", full_review_text)
        if review_identity in seen_keys:
            continue
        seen_keys.add(review_identity)

        parsed_reviews.append(
            ParsedReview(
                review_term_label=review_term,
                updated_at=updated_at,
                professor_review_text=professor_review_text,
                course_review_text=course_review_text,
                full_review_text=full_review_text,
                sentiment_score=overall_score,
                professor_sentiment_score=professor_score,
                course_sentiment_score=course_score,
                sentiment_label=sentiment_label(overall_score),
            )
        )

    return parsed_reviews


def match_professor_id(conn, professor_name: str) -> int | None:
    normalized = normalize_professor_name(professor_name)
    candidates = conn.execute(
        text(
            """
            SELECT professor_id, name
            FROM professor
            """
        )
    ).mappings().all()

    exact_matches = [
        row["professor_id"]
        for row in candidates
        if normalize_professor_name(str(row["name"])).lower() == normalized.lower()
    ]
    if len(exact_matches) == 1:
        return int(exact_matches[0])
    return None


def build_review_rows(
    *,
    course_id: int,
    subject: str,
    catalog_nbr: str,
    professor_name: str,
    professor_page_url: str,
    fetched_at: datetime,
    reviews: Iterable[ParsedReview],
    professor_id: int | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    fetched_at_text = fetched_at.isoformat()

    for review in reviews:
        review_key_material = "|".join(
            [
                str(course_id),
                professor_page_url,
                review.review_term_label or "",
                review.updated_at or "",
                review.full_review_text,
            ]
        )
        review_key = hashlib.sha1(review_key_material.encode("utf-8")).hexdigest()
        rows.append(
            {
                "review_key": review_key,
                "course_id": course_id,
                "subject": subject,
                "catalog_nbr": catalog_nbr,
                "professor_id": professor_id,
                "professor_name": professor_name,
                "professor_page_url": professor_page_url,
                "review_term_label": review.review_term_label,
                "updated_at": review.updated_at,
                "professor_review_text": review.professor_review_text,
                "course_review_text": review.course_review_text,
                "full_review_text": review.full_review_text,
                "sentiment_score": review.sentiment_score,
                "professor_sentiment_score": review.professor_sentiment_score,
                "course_sentiment_score": review.course_sentiment_score,
                "sentiment_label": review.sentiment_label,
                "fetched_at": fetched_at_text,
            }
        )
    return rows


def summarize_review_rows(rows: list[dict[str, object]]) -> dict[str, object] | None:
    if not rows:
        return None

    review_count = len(rows)
    positive_count = sum(1 for row in rows if row["sentiment_label"] == "positive")
    neutral_count = sum(1 for row in rows if row["sentiment_label"] == "neutral")
    negative_count = sum(1 for row in rows if row["sentiment_label"] == "negative")

    avg_sentiment = sum(float(row["sentiment_score"]) for row in rows) / review_count

    prof_scores = [float(row["professor_sentiment_score"]) for row in rows if row["professor_sentiment_score"] is not None]
    course_scores = [float(row["course_sentiment_score"]) for row in rows if row["course_sentiment_score"] is not None]

    # Weight volume and polarity together so a course-professor pair with many
    # positive reviews gets a stronger demand signal than a single favorable review.
    sentiment_demand_score = math.log1p(review_count) * (avg_sentiment + 1.0)
    most_recent_review_at = max((clean_text(str(row["updated_at"])) for row in rows if row["updated_at"]), default=None)

    sample = rows[0]
    return {
        "course_id": sample["course_id"],
        "professor_id": sample["professor_id"],
        "subject": sample["subject"],
        "catalog_nbr": sample["catalog_nbr"],
        "professor_name": sample["professor_name"],
        "professor_page_url": sample["professor_page_url"],
        "review_count": review_count,
        "positive_review_count": positive_count,
        "neutral_review_count": neutral_count,
        "negative_review_count": negative_count,
        "avg_sentiment_score": avg_sentiment,
        "avg_professor_sentiment_score": (sum(prof_scores) / len(prof_scores)) if prof_scores else None,
        "avg_course_sentiment_score": (sum(course_scores) / len(course_scores)) if course_scores else None,
        "sentiment_demand_score": sentiment_demand_score,
        "most_recent_review_at": most_recent_review_at,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
