from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DAG_HELPER_DIR = ROOT_DIR / "etl" / "airflow" / "dags"
if str(DAG_HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(DAG_HELPER_DIR))

from major_requirement_pipeline import normalize_scraped_major_requirements, scrape_requirement_sources


def normalize_major_requirements(database_url: str | None = None) -> dict[str, int]:
    database_url = database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise KeyError("DATABASE_URL")
    return normalize_scraped_major_requirements(database_url)


def scrape_major_requirements(database_url: str | None = None) -> dict[str, int]:
    database_url = database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise KeyError("DATABASE_URL")
    return scrape_requirement_sources(database_url)


if __name__ == "__main__":
    scrape_major_requirements()
    normalize_major_requirements()
