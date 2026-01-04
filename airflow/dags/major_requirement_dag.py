
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG
from annotated_types import doc
import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from airflow.models import Variable
import psycopg
from psycopg.rows import dict_row

#automating grabbing of the pdf fiels for major requirement sheets
#define the sources of the pdfs

gen_ed_requirement_pdf_sources = {
     "https://gened.as.virginia.edu/sites/default/files/2025-04/Engagements-Pathway-Checklist.pdf": "arts_and_science_requirements.pdf",

}

pull_major_requirement_pdf_sources = {
     "https://records.ureg.virginia.edu/preview_program.php?catoid=58&poid=8028": "major_requirements_AAS.pdf"
     
}


def pull_major_requirement_pdfs(**kwargs):

    return
def pull_general_education_requirement_pdfs(**kwargs):

    return


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
        pull_major_requirement_pdfs_task = PythonOperator(
            task_id="pull_major_requirement_pdfs",
            python_callable=pull_major_requirement_pdfs,
            op_kwargs={"term": "1262"},  # SPRING 2026: the most recent term 
        )

        pull_general_education_requirement_pdfs_task = PythonOperator(
            task_id="pull_general_education_requirement_pdfs",
            python_callable=pull_general_education_requirement_pdfs,
            op_kwargs={"term": "1262"},  # SPRING 2026: the most recent term 
        )
        pull_major_requirement_pdfs_task
