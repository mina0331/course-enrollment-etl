from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from major_requirement_pipeline import (
    normalize_scraped_major_requirements,
    scrape_requirement_sources,
)


def fetch_major_requirement_page_html(**kwargs):
    result = scrape_requirement_sources()
    result["term"] = kwargs.get("term")
    result["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return result

def pull_general_education_requirement_pdfs(**kwargs):
    return {
        "status": "covered_by_fetch_requirement_page_html",
        "term": kwargs.get("term"),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def normalize_major_requirements_task(**kwargs):
    result = normalize_scraped_major_requirements()
    result["status"] = "ok"
    result["normalized_at"] = datetime.now(timezone.utc).isoformat()
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    default_args=default_args,
    dag_id="major_requirement_dag",
    description="DAG to pull major requirement PDFs",
    schedule=timedelta(days=30),  # Run monthly

) as dag:
        fetch_major_requirement_page_html_task = PythonOperator(
            task_id="fetch_major_requirement_page_html",
            python_callable=fetch_major_requirement_page_html,
            op_kwargs={"term": "1262"},  # SPRING 2026: the most recent term 
        )

        pull_general_education_requirement_pdfs_task = PythonOperator(
            task_id="pull_general_education_requirement_pdfs",
            python_callable=pull_general_education_requirement_pdfs,
            op_kwargs={"term": "1262"},  # SPRING 2026: the most recent term 
        )
        normalize_major_requirements_task = PythonOperator(
            task_id="normalize_major_requirements",
            python_callable=normalize_major_requirements_task,
        )

        fetch_major_requirement_page_html_task >> pull_general_education_requirement_pdfs_task >> normalize_major_requirements_task
