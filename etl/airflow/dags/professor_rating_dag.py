from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import urllib.request

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import create_engine, text

from course_forum_reviews import (
    COURSE_FORUM_BASE_URL,
    build_review_rows,
    ensure_course_forum_review_tables,
    extract_course_professor_links,
    match_professor_id,
    normalize_professor_name,
    parse_course_professor_page,
    summarize_review_rows,
)

PAGE_BATCH_SIZE = 100
REVIEW_BATCH_SIZE = 100
RAW_PAGE_BATCH_SIZE = 50


def fetch_url(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="replace")


def get_database_engine():
    return create_engine(os.environ["DATABASE_URL"])


def flush_professor_page_rows(conn, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO course_forum_professor_page_raw_html (
                course_id,
                subject,
                catalog_nbr,
                professor_name,
                professor_page_url,
                raw_html,
                fetched_at
            )
            VALUES (
                :course_id,
                :subject,
                :catalog_nbr,
                :professor_name,
                :professor_page_url,
                :raw_html,
                :fetched_at
            )
            ON CONFLICT (professor_page_url) DO UPDATE SET
                raw_html = EXCLUDED.raw_html,
                fetched_at = EXCLUDED.fetched_at,
                professor_name = EXCLUDED.professor_name,
                course_id = EXCLUDED.course_id,
                subject = EXCLUDED.subject,
                catalog_nbr = EXCLUDED.catalog_nbr
            """
        ),
        rows,
    )


def flush_review_rows(conn, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO course_forum_review (
                review_key,
                course_id,
                subject,
                catalog_nbr,
                professor_id,
                professor_name,
                professor_page_url,
                review_term_label,
                updated_at,
                professor_review_text,
                course_review_text,
                full_review_text,
                sentiment_score,
                professor_sentiment_score,
                course_sentiment_score,
                sentiment_label,
                fetched_at
            )
            VALUES (
                :review_key,
                :course_id,
                :subject,
                :catalog_nbr,
                :professor_id,
                :professor_name,
                :professor_page_url,
                :review_term_label,
                :updated_at,
                :professor_review_text,
                :course_review_text,
                :full_review_text,
                :sentiment_score,
                :professor_sentiment_score,
                :course_sentiment_score,
                :sentiment_label,
                :fetched_at
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
            INSERT INTO course_forum_review_summary (
                course_id,
                professor_id,
                subject,
                catalog_nbr,
                professor_name,
                professor_page_url,
                review_count,
                positive_review_count,
                neutral_review_count,
                negative_review_count,
                avg_sentiment_score,
                avg_professor_sentiment_score,
                avg_course_sentiment_score,
                sentiment_demand_score,
                most_recent_review_at,
                fetched_at
            )
            VALUES (
                :course_id,
                :professor_id,
                :subject,
                :catalog_nbr,
                :professor_name,
                :professor_page_url,
                :review_count,
                :positive_review_count,
                :neutral_review_count,
                :negative_review_count,
                :avg_sentiment_score,
                :avg_professor_sentiment_score,
                :avg_course_sentiment_score,
                :sentiment_demand_score,
                :most_recent_review_at,
                :fetched_at
            )
            ON CONFLICT (course_id, professor_name) DO UPDATE SET
                professor_id = EXCLUDED.professor_id,
                subject = EXCLUDED.subject,
                catalog_nbr = EXCLUDED.catalog_nbr,
                professor_page_url = EXCLUDED.professor_page_url,
                review_count = EXCLUDED.review_count,
                positive_review_count = EXCLUDED.positive_review_count,
                neutral_review_count = EXCLUDED.neutral_review_count,
                negative_review_count = EXCLUDED.negative_review_count,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                avg_professor_sentiment_score = EXCLUDED.avg_professor_sentiment_score,
                avg_course_sentiment_score = EXCLUDED.avg_course_sentiment_score,
                sentiment_demand_score = EXCLUDED.sentiment_demand_score,
                most_recent_review_at = EXCLUDED.most_recent_review_at,
                fetched_at = EXCLUDED.fetched_at
            """
        ),
        rows,
    )


def build_professor_lookup(conn) -> dict[str, int | None]:
    rows = conn.execute(
        text(
            """
            SELECT professor_id, name
            FROM professor
            """
        )
    ).mappings().all()

    lookup: dict[str, int | None] = {}
    for row in rows:
        normalized = normalize_professor_name(str(row["name"])).lower()
        professor_id = int(row["professor_id"])
        if normalized in lookup and lookup[normalized] != professor_id:
            lookup[normalized] = None
        else:
            lookup[normalized] = professor_id
    return lookup


def fetch_course_forum_raw_page_batch(conn, *, offset: int, batch_size: int = RAW_PAGE_BATCH_SIZE):
    return conn.execute(
        text(
            """
            SELECT course_id, subject, catalog_nbr, professor_name, professor_page_url, raw_html, fetched_at
            FROM course_forum_professor_page_raw_html
            WHERE raw_html <> ''
            ORDER BY professor_page_url
            LIMIT :limit OFFSET :offset
            """
        ),
        {"limit": batch_size, "offset": offset},
    ).mappings().all()


def fetching_courses_to_pull_in_for(conn) -> list[dict[str, object]]:
    results = conn.execute(
        text(
            """
            SELECT subject_id, catalog_nbr, course_id
            FROM course
            ORDER BY subject_id, catalog_nbr, course_id
            """
        )
    ).mappings()
    return [
        {
            "subject": row["subject_id"],
            "catalog_nbr": str(row["catalog_nbr"]),
            "course_id": row["course_id"],
        }
        for row in results
    ]


def section_professor_uses_course_id(conn) -> bool:
    dialect = conn.engine.dialect.name
    if dialect == "sqlite":
        rows = conn.execute(text("PRAGMA table_info(section_professor)")).mappings().all()
        columns = {row["name"] for row in rows}
    else:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'section_professor'
                """
            )
        ).mappings().all()
        columns = {row["column_name"] for row in rows}
    return "course_id" in columns


def upsert_professor_rating(conn, *, term_id: str, course_id: int, professor_id: int, rating: str | None, difficulty: str | None) -> None:
    uses_course_id = section_professor_uses_course_id(conn)
    updates = {"rating": rating, "difficulty": difficulty, "professor_id": professor_id, "course_id": course_id, "term_id": term_id}

    if uses_course_id:
        if rating is not None:
            conn.execute(
                text(
                    """
                    UPDATE section_professor
                    SET rating = :rating
                    WHERE professor_id = :professor_id
                      AND course_id = :course_id
                      AND term_id = :term_id
                    """
                ),
                updates,
            )
        if difficulty is not None:
            conn.execute(
                text(
                    """
                    UPDATE section_professor
                    SET difficulty = :difficulty
                    WHERE professor_id = :professor_id
                      AND course_id = :course_id
                      AND term_id = :term_id
                    """
                ),
                updates,
            )
        return

    if rating is not None:
        conn.execute(
            text(
                """
                UPDATE section_professor
                SET rating = :rating
                WHERE professor_id = :professor_id
                  AND term_id = :term_id
                  AND class_nbr IN (
                      SELECT class_nbr
                      FROM section
                      WHERE term_id = :term_id
                        AND course_id = :course_id
                  )
                """
            ),
            updates,
        )
    if difficulty is not None:
        conn.execute(
            text(
                """
                UPDATE section_professor
                SET difficulty = :difficulty
                WHERE professor_id = :professor_id
                  AND term_id = :term_id
                  AND class_nbr IN (
                      SELECT class_nbr
                      FROM section
                      WHERE term_id = :term_id
                        AND course_id = :course_id
                  )
                """
            ),
            updates,
        )


def pull_professor_rating_raw_html(**kwargs):
    term = kwargs.get("term")
    engine = get_database_engine()

    with engine.begin() as conn:
        ensure_course_forum_review_tables(conn)
        courses = fetching_courses_to_pull_in_for(conn)
        rows = []
        for course in courses:
            url_course_forum = f"{COURSE_FORUM_BASE_URL}/course/{course['subject'].upper()}/{course['catalog_nbr']}/All"
            fetched_at = datetime.now(timezone.utc).isoformat()
            try:
                raw_html = fetch_url(url_course_forum)
            except Exception:
                raw_html = ""

            rows.append(
                {
                    "term": term,
                    "subject": course["subject"],
                    "catalog_nbr": course["catalog_nbr"],
                    "course_id": course["course_id"],
                    "url": url_course_forum,
                    "raw_html": raw_html,
                    "fetched_at": fetched_at,
                }
            )

        if rows:
            conn.execute(
                text(
                    """
                    INSERT INTO professor_rating_raw_html (
                        term,
                        subject,
                        catalog_nbr,
                        course_id,
                        url,
                        raw_html,
                        fetched_at
                    )
                    VALUES (
                        :term,
                        :subject,
                        :catalog_nbr,
                        :course_id,
                        :url,
                        :raw_html,
                        :fetched_at
                    )
                    ON CONFLICT (subject, catalog_nbr, course_id) DO UPDATE SET
                        raw_html = EXCLUDED.raw_html,
                        fetched_at = EXCLUDED.fetched_at,
                        term = EXCLUDED.term,
                        url = EXCLUDED.url
                    """
                ),
                rows,
            )


def pull_course_professor_review_pages(**kwargs):
    engine = get_database_engine()
    with engine.begin() as conn:
        ensure_course_forum_review_tables(conn)
        course_pages = conn.execute(
            text(
                """
                SELECT subject, catalog_nbr, course_id, raw_html
                FROM professor_rating_raw_html
                WHERE raw_html <> ''
                """
            )
        ).mappings().all()

        page_rows = []
        total_pages_written = 0
        for row in course_pages:
            professor_links = extract_course_professor_links(row["raw_html"])
            for link in professor_links:
                fetched_at = datetime.now(timezone.utc).isoformat()
                try:
                    raw_html = fetch_url(link.professor_page_url)
                except Exception:
                    raw_html = ""

                page_rows.append(
                    {
                        "course_id": row["course_id"],
                        "subject": row["subject"],
                        "catalog_nbr": row["catalog_nbr"],
                        "professor_name": normalize_professor_name(link.professor_name),
                        "professor_page_url": link.professor_page_url,
                        "raw_html": raw_html,
                        "fetched_at": fetched_at,
                    }
                )
                if len(page_rows) >= PAGE_BATCH_SIZE:
                    flush_professor_page_rows(conn, page_rows)
                    total_pages_written += len(page_rows)
                    print(f"Wrote {total_pages_written} professor review pages so far")
                    page_rows.clear()

        if page_rows:
            flush_professor_page_rows(conn, page_rows)
            total_pages_written += len(page_rows)
            print(f"Wrote {total_pages_written} professor review pages total")


def transform_html_file_instructor(**kwargs):
    term = kwargs.get("term")
    engine = get_database_engine()

    with engine.begin() as conn:
        ensure_course_forum_review_tables(conn)
        course_pages = conn.execute(
            text(
                """
                SELECT subject, catalog_nbr, course_id, raw_html
                FROM professor_rating_raw_html
                WHERE raw_html <> ''
                """
            )
        ).mappings().all()

        for row in course_pages:
            professor_links = extract_course_professor_links(row["raw_html"])
            print(f"Processing course {row['subject']} {row['catalog_nbr']} with {len(professor_links)} instructors")

            for link in professor_links:
                professor_name = normalize_professor_name(link.professor_name)
                professor_id = match_professor_id(conn, professor_name)

                if professor_id is None:
                    print(f"WARNING: No unique professor found with name {professor_name}. Skipping numeric rating update for this instructor.")
                    continue

                # Try to recover rating + difficulty from the course listing page.
                listing_html = row["raw_html"]
                rating = None
                difficulty = None
                if listing_html:
                    try:
                        from bs4 import BeautifulSoup

                        soup = BeautifulSoup(listing_html, "html.parser")
                        for container in soup.find_all(["li", "div", "section", "article"]):
                            container_text = container.get_text(" ", strip=True)
                            if professor_name.lower() not in container_text.lower():
                                continue
                            rating_tag = container.find("p", id="rating")
                            difficulty_tag = container.find("p", id="difficulty")
                            rating = rating_tag.get_text(strip=True) if rating_tag else None
                            difficulty = difficulty_tag.get_text(strip=True) if difficulty_tag else None
                            break
                    except Exception:
                        rating = None
                        difficulty = None

                if rating == "—":
                    rating = None
                if difficulty == "—":
                    difficulty = None

                if rating is not None or difficulty is not None:
                    upsert_professor_rating(
                        conn,
                        term_id=term,
                        course_id=int(row["course_id"]),
                        professor_id=int(professor_id),
                        rating=rating,
                        difficulty=difficulty,
                    )


def parse_and_score_course_forum_reviews(**kwargs):
    engine = get_database_engine()
    with engine.begin() as conn:
        ensure_course_forum_review_tables(conn)
        professor_lookup = build_professor_lookup(conn)
        conn.execute(text("DELETE FROM course_forum_review"))
        conn.execute(text("DELETE FROM course_forum_review_summary"))
    total_reviews_written = 0
    total_summaries_written = 0
    processed_pages = 0
    offset = 0

    while True:
        with engine.begin() as conn:
            raw_page_batch = fetch_course_forum_raw_page_batch(
                conn,
                offset=offset,
                batch_size=RAW_PAGE_BATCH_SIZE,
            )
        if not raw_page_batch:
            break

        review_rows = []
        summary_rows = []
        for page in raw_page_batch:
            professor_name = normalize_professor_name(str(page["professor_name"]))
            professor_id = professor_lookup.get(professor_name.lower())
            reviews = parse_course_professor_page(str(page["raw_html"]), professor_name)
            if not reviews:
                processed_pages += 1
                continue

            built_rows = build_review_rows(
                course_id=int(page["course_id"]),
                subject=str(page["subject"]),
                catalog_nbr=str(page["catalog_nbr"]),
                professor_name=professor_name,
                professor_page_url=str(page["professor_page_url"]),
                fetched_at=datetime.now(timezone.utc),
                reviews=reviews,
                professor_id=professor_id,
            )
            review_rows.extend(built_rows)
            summary = summarize_review_rows(built_rows)
            if summary is not None:
                summary_rows.append(summary)
            processed_pages += 1

        with engine.begin() as conn:
            if review_rows:
                flush_review_rows(conn, review_rows)
                total_reviews_written += len(review_rows)
            if summary_rows:
                flush_summary_rows(conn, summary_rows)
                total_summaries_written += len(summary_rows)

        offset += len(raw_page_batch)
        print(
            f"Processed {processed_pages} professor review pages so far; "
            f"wrote {total_reviews_written} reviews and {total_summaries_written} summaries"
        )

    print(f"Finished parsing {total_reviews_written} reviews and {total_summaries_written} summaries")


default_args = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "fetching_raw_html_from_course_forum",
    default_args=default_args,
    description="Fetch Course Forum pages, extract course-professor review text, and score sentiment",
    schedule=timedelta(days=7),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    fetch_course_pages = PythonOperator(
        task_id="fetching_the_raw_html_file_for_professor_rating",
        op_kwargs={"term": "1262"},
        python_callable=pull_professor_rating_raw_html,
    )

    fetch_professor_pages = PythonOperator(
        task_id="fetching_course_professor_review_pages",
        op_kwargs={"term": "1262"},
        python_callable=pull_course_professor_review_pages,
    )

    update_instructor_numeric_ratings = PythonOperator(
        task_id="transforming_html_file_to_get_instructor_rating",
        op_kwargs={"term": "1262"},
        python_callable=transform_html_file_instructor,
    )

    parse_review_sentiment = PythonOperator(
        task_id="parsing_course_professor_reviews_and_sentiment",
        op_kwargs={"term": "1262"},
        python_callable=parse_and_score_course_forum_reviews,
    )

    fetch_course_pages >> fetch_professor_pages >> update_instructor_numeric_ratings >> parse_review_sentiment
