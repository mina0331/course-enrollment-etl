from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text


SOURCE_DEFINITIONS = [
    {
        "school": "SEAS",
        "program_type": "major",
        "program_code": "BSCS",
        "program_name": "Computer Science, B.S.",
        "source_url": "https://engineering.virginia.edu/department/computer-science/academics/undergraduate-programs/bs-computer-science",
        "source_title": "UVA Engineering Computer Science BS Requirements",
        "source_format": "html",
        "source_type": "department_page",
        "confidence": "medium",
    },
    {
        "school": "SEAS",
        "program_type": "major",
        "program_code": "BSCE",
        "program_name": "Computer Engineering, B.S.",
        "source_url": "https://engineering.virginia.edu/offices-programs/computer-engineering-program/academics/undergraduate-program/bs-computer-engineering",
        "source_title": "UVA Engineering Computer Engineering BS Requirements",
        "source_format": "html",
        "source_type": "department_page",
        "confidence": "medium",
    },
    {
        "school": "SEAS",
        "program_type": "major",
        "program_code": "BSEE",
        "program_name": "Electrical Engineering, B.S.",
        "source_url": "https://engineering.virginia.edu/department/electrical-and-computer-engineering/academics/undergraduate-programs/bs-electrical-engineering",
        "source_title": "UVA Engineering Electrical Engineering BS Requirements",
        "source_format": "html",
        "source_type": "department_page",
        "confidence": "medium",
    },
]


COURSE_PATTERN = re.compile(r"\b([A-Z]{2,5})\s+(\d{4})\b")


@dataclass
class ParsedRequirement:
    group_code: str
    group_name: str
    group_type: str
    rule_code: str
    rule_label: str
    rule_type: str
    text_requirement: str
    courses: list[dict[str, object]]


def fetch_html(url: str) -> str:
    import urllib.request

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def html_to_text(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())


def extract_courses(text_value: str) -> list[dict[str, object]]:
    courses = []
    for subject_id, catalog_nbr in COURSE_PATTERN.findall(text_value.upper()):
        courses.append(
            {
                "subject_id": subject_id,
                "catalog_nbr": int(catalog_nbr),
                "match_type": "explicit_course",
            }
        )
    return courses


def parse_engineering_html(program_code: str, raw_html: str) -> list[ParsedRequirement]:
    soup = BeautifulSoup(raw_html, "html.parser")
    parsed: list[ParsedRequirement] = []

    headings = soup.find_all(["h2", "h3", "h4"])
    for heading_index, heading in enumerate(headings, start=1):
        group_name = heading.get_text(" ", strip=True)
        if not group_name:
            continue

        sibling = heading.find_next_sibling()
        bullet_texts: list[str] = []
        while sibling and sibling.name not in {"h2", "h3", "h4"}:
            if sibling.name in {"ul", "ol"}:
                bullet_texts.extend(
                    li.get_text(" ", strip=True) for li in sibling.find_all("li", recursive=True)
                )
            elif sibling.name == "p":
                bullet_texts.append(sibling.get_text(" ", strip=True))
            sibling = sibling.find_next_sibling()

        cleaned_bullets = [text for text in bullet_texts if text]
        if not cleaned_bullets:
            continue

        group_code = f"{program_code}_group_{heading_index}"
        for rule_index, bullet in enumerate(cleaned_bullets, start=1):
            parsed.append(
                ParsedRequirement(
                    group_code=group_code,
                    group_name=group_name,
                    group_type="course_block",
                    rule_code=f"{group_code}_rule_{rule_index}",
                    rule_label=bullet[:120],
                    rule_type="course_list" if extract_courses(bullet) else "text_rule",
                    text_requirement=bullet,
                    courses=extract_courses(bullet),
                )
            )

    return parsed


def upsert_source(conn, source: dict[str, object], raw_html: str, raw_text: str) -> int:
    fetched_at = datetime.now(timezone.utc)
    source_id = conn.execute(
        text(
            """
            INSERT INTO requirement_source (
                source_type,
                school,
                program_type,
                program_code,
                program_name,
                source_url,
                source_title,
                source_format,
                confidence,
                fetched_at
            )
            VALUES (
                :source_type,
                :school,
                :program_type,
                :program_code,
                :program_name,
                :source_url,
                :source_title,
                :source_format,
                :confidence,
                :fetched_at
            )
            ON CONFLICT (program_type, program_code, catalog_year, source_url) DO UPDATE
            SET
                program_name = EXCLUDED.program_name,
                source_title = EXCLUDED.source_title,
                confidence = EXCLUDED.confidence,
                fetched_at = EXCLUDED.fetched_at
            RETURNING source_id
            """
        ),
        {**source, "fetched_at": fetched_at},
    ).scalar_one()

    conn.execute(
        text("DELETE FROM requirement_source_raw WHERE source_id = :source_id"),
        {"source_id": source_id},
    )
    conn.execute(
        text(
            """
            INSERT INTO requirement_source_raw (
                source_id,
                raw_html,
                raw_text,
                raw_json,
                parse_status
            )
            VALUES (:source_id, :raw_html, :raw_text, CAST(:raw_json AS jsonb), 'parsed')
            """
        ),
        {
            "source_id": source_id,
            "raw_html": raw_html,
            "raw_text": raw_text,
            "raw_json": json.dumps({"captured_from": source["source_url"]}),
        },
    )
    return int(source_id)


def upsert_program(conn, source: dict[str, object]) -> int:
    return int(
        conn.execute(
            text(
                """
                INSERT INTO academic_program (
                    school,
                    program_type,
                    program_code,
                    program_name,
                    degree
                )
                VALUES (:school, :program_type, :program_code, :program_name, NULL)
                ON CONFLICT (program_code) DO UPDATE
                SET
                    school = EXCLUDED.school,
                    program_type = EXCLUDED.program_type,
                    program_name = EXCLUDED.program_name
                RETURNING program_id
                """
            ),
            source,
        ).scalar_one()
    )


def link_course_id(conn, subject_id: str, catalog_nbr: int) -> int | None:
    return conn.execute(
        text(
            """
            SELECT course_id
            FROM course
            WHERE subject_id = :subject_id
              AND catalog_nbr = :catalog_nbr
            ORDER BY course_id
            LIMIT 1
            """
        ),
        {"subject_id": subject_id, "catalog_nbr": catalog_nbr},
    ).scalar_one_or_none()


def write_requirements(
    conn,
    *,
    program_id: int,
    source_id: int,
    parsed_requirements: Iterable[ParsedRequirement],
) -> tuple[int, int, int]:
    groups_written = 0
    rules_written = 0
    courses_written = 0
    group_cache: dict[str, int] = {}

    for display_order, requirement in enumerate(parsed_requirements, start=1):
        group_id = group_cache.get(requirement.group_code)
        if group_id is None:
            group_id = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO requirement_group (
                            program_id,
                            group_code,
                            group_name,
                            group_type,
                            display_order
                        )
                        VALUES (
                            :program_id,
                            :group_code,
                            :group_name,
                            :group_type,
                            :display_order
                        )
                        ON CONFLICT (program_id, group_code) DO UPDATE
                        SET
                            group_name = EXCLUDED.group_name,
                            group_type = EXCLUDED.group_type,
                            display_order = EXCLUDED.display_order
                        RETURNING group_id
                        """
                    ),
                    {
                        "program_id": program_id,
                        "group_code": requirement.group_code,
                        "group_name": requirement.group_name,
                        "group_type": requirement.group_type,
                        "display_order": display_order,
                    },
                ).scalar_one()
            )
            group_cache[requirement.group_code] = group_id
            groups_written += 1

        rule_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO requirement_rule (
                        group_id,
                        source_id,
                        rule_code,
                        rule_label,
                        rule_type,
                        text_requirement,
                        display_order
                    )
                    VALUES (
                        :group_id,
                        :source_id,
                        :rule_code,
                        :rule_label,
                        :rule_type,
                        :text_requirement,
                        :display_order
                    )
                    ON CONFLICT (group_id, rule_code) DO UPDATE
                    SET
                        rule_label = EXCLUDED.rule_label,
                        rule_type = EXCLUDED.rule_type,
                        text_requirement = EXCLUDED.text_requirement,
                        display_order = EXCLUDED.display_order
                    RETURNING rule_id
                    """
                ),
                {
                    "group_id": group_id,
                    "source_id": source_id,
                    "rule_code": requirement.rule_code,
                    "rule_label": requirement.rule_label,
                    "rule_type": requirement.rule_type,
                    "text_requirement": requirement.text_requirement,
                    "display_order": display_order,
                },
            ).scalar_one()
        )
        rules_written += 1

        conn.execute(text("DELETE FROM requirement_course WHERE rule_id = :rule_id"), {"rule_id": rule_id})
        for course in requirement.courses:
            conn.execute(
                text(
                    """
                    INSERT INTO requirement_course (
                        rule_id,
                        course_id,
                        subject_id,
                        catalog_nbr,
                        match_type
                    )
                    VALUES (
                        :rule_id,
                        :course_id,
                        :subject_id,
                        :catalog_nbr,
                        :match_type
                    )
                    """
                ),
                {
                    "rule_id": rule_id,
                    "course_id": link_course_id(conn, course["subject_id"], course["catalog_nbr"]),
                    "subject_id": course["subject_id"],
                    "catalog_nbr": course["catalog_nbr"],
                    "match_type": course["match_type"],
                },
            )
            courses_written += 1

    return groups_written, rules_written, courses_written


def normalize_major_requirements(database_url: str | None = None) -> None:
    database_url = database_url or os.environ["DATABASE_URL"]
    engine = create_engine(database_url)

    for source in SOURCE_DEFINITIONS:
        raw_html = fetch_html(str(source["source_url"]))
        raw_text = html_to_text(raw_html)

        with engine.begin() as conn:
            source_id = upsert_source(conn, source, raw_html, raw_text)
            program_id = upsert_program(conn, source)

            run_id = conn.execute(
                text(
                    """
                    INSERT INTO requirement_normalization_run (
                        source_id,
                        parser_name,
                        run_status
                    )
                    VALUES (:source_id, :parser_name, 'running')
                    RETURNING run_id
                    """
                ),
                {"source_id": source_id, "parser_name": "parse_engineering_html"},
            ).scalar_one()

            try:
                parsed = parse_engineering_html(str(source["program_code"]), raw_html)
                groups_written, rules_written, courses_written = write_requirements(
                    conn,
                    program_id=program_id,
                    source_id=source_id,
                    parsed_requirements=parsed,
                )
                conn.execute(
                    text(
                        """
                        UPDATE requirement_normalization_run
                        SET
                            run_status = 'success',
                            groups_written = :groups_written,
                            rules_written = :rules_written,
                            courses_written = :courses_written,
                            completed_at = CURRENT_TIMESTAMP
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "run_id": run_id,
                        "groups_written": groups_written,
                        "rules_written": rules_written,
                        "courses_written": courses_written,
                    },
                )
            except Exception as exc:
                conn.execute(
                    text(
                        """
                        UPDATE requirement_source_raw
                        SET
                            parse_status = 'failed',
                            parse_error = :parse_error
                        WHERE source_id = :source_id
                        """
                    ),
                    {"source_id": source_id, "parse_error": str(exc)},
                )
                conn.execute(
                    text(
                        """
                        UPDATE requirement_normalization_run
                        SET
                            run_status = 'failed',
                            completed_at = CURRENT_TIMESTAMP
                        WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id},
                )
                raise


if __name__ == "__main__":
    normalize_major_requirements()
