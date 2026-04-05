import os
from functools import lru_cache

from flask import Flask, render_template, request
from sqlalchemy import create_engine, text


DEFAULT_DATABASE_URL = "postgresql+psycopg2://app:app@localhost:5433/appdb"
DEFAULT_TERM_ID = "1262"

app = Flask(__name__)


@lru_cache(maxsize=1)
def get_engine():
    return create_engine(
        os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        pool_pre_ping=True,
    )


def term_label(term_id: str) -> str:
    if len(term_id) != 4 or not term_id.isdigit():
        return term_id

    year = f"20{term_id[1:3]}"
    semester_code = term_id[-1]
    semester_name = {
        "1": "J-Term",
        "2": "Spring",
        "3": "Summer",
        "4": "Fall",
        "8": "Fall",
    }.get(semester_code, "Unknown")
    return f"{semester_name} {year}"


def fetch_available_terms():
    query = text(
        """
        SELECT DISTINCT term_id
        FROM section
        WHERE term_id IS NOT NULL
        ORDER BY term_id DESC
        """
    )

    with get_engine().connect() as conn:
        term_ids = [row["term_id"] for row in conn.execute(query).mappings().all()]

    if DEFAULT_TERM_ID not in term_ids:
        term_ids.insert(0, DEFAULT_TERM_ID)

    return [{"value": term_id, "label": term_label(term_id)} for term_id in term_ids]


def fetch_dashboard_rows(term_id: str, search: str):
    query = text(
        """
        WITH course_sentiment AS (
            SELECT
                course_id,
                AVG(sentiment_demand_score) AS demand_score,
                AVG(avg_sentiment_score) AS overall_sentiment,
                SUM(review_count) AS total_reviews
            FROM course_forum_review_summary
            GROUP BY course_id
        ),
        term_professors AS (
            SELECT
                sp.term_id,
                sp.course_id,
                STRING_AGG(DISTINCT p.name, ', ' ORDER BY p.name) AS professor_names
            FROM section_professor sp
            JOIN professor p
                ON p.professor_id = sp.professor_id
            GROUP BY sp.term_id, sp.course_id
        )
        SELECT
            s.class_nbr,
            s.course_id,
            c.subject_id,
            c.catalog_nbr,
            c.title,
            s.enrollment_status,
            s.capacity,
            s.seats_taken,
            s.current_waitlist,
            GREATEST(COALESCE(s.capacity, 0) - COALESCE(s.seats_taken, 0), 0) AS seats_open,
            COALESCE(tp.professor_names, 'TBA') AS professor_names,
            cs.demand_score,
            cs.overall_sentiment,
            COALESCE(cs.total_reviews, 0) AS total_reviews
        FROM section s
        JOIN course c
            ON c.course_id = s.course_id
        LEFT JOIN course_sentiment cs
            ON cs.course_id = s.course_id
        LEFT JOIN term_professors tp
            ON tp.term_id = s.term_id
           AND tp.course_id = s.course_id
        WHERE s.term_id = :term_id
          AND (
                :search = ''
                OR c.subject_id ILIKE :search_pattern
                OR CAST(c.catalog_nbr AS TEXT) ILIKE :search_pattern
                OR c.title ILIKE :search_pattern
                OR CAST(s.class_nbr AS TEXT) ILIKE :search_pattern
                OR COALESCE(tp.professor_names, '') ILIKE :search_pattern
          )
        ORDER BY
            c.subject_id,
            c.catalog_nbr,
            s.class_nbr
        """
    )

    with get_engine().connect() as conn:
        rows = conn.execute(
            query,
            {
                "term_id": term_id,
                "search": search.strip(),
                "search_pattern": f"%{search.strip()}%",
            },
        ).mappings().all()
    return rows


@app.route("/")
def dashboard():
    available_terms = fetch_available_terms()
    available_term_ids = {term["value"] for term in available_terms}
    requested_term_id = request.args.get("term", DEFAULT_TERM_ID).strip() or DEFAULT_TERM_ID
    term_id = requested_term_id if requested_term_id in available_term_ids else DEFAULT_TERM_ID
    search = request.args.get("q", "").strip()
    rows = fetch_dashboard_rows(term_id=term_id, search=search)

    total_courses = len({(row["subject_id"], row["catalog_nbr"]) for row in rows})
    sections_with_reviews = sum(1 for row in rows if row["total_reviews"] > 0)
    open_sections = sum(1 for row in rows if row["enrollment_status"] == "O")

    return render_template(
        "dashboard.html",
        rows=rows,
        available_terms=available_terms,
        term_id=term_id,
        term_name=term_label(term_id),
        search=search,
        total_sections=len(rows),
        total_courses=total_courses,
        open_sections=open_sections,
        sections_with_reviews=sections_with_reviews,
    )


if __name__ == "__main__":
    app.run(debug=True)
