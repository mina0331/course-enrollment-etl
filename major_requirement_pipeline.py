from etl.airflow.dags.major_requirement_pipeline import (
    normalize_scraped_major_requirements,
    scrape_requirement_sources,
)

__all__ = [
    "normalize_scraped_major_requirements",
    "scrape_requirement_sources",
]
