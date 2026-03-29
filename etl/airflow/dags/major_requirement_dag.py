from pathlib import Path
import sys
import urllib
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.normalize_major_requirements import normalize_major_requirements

#automating grabbing of the pdf fiels for major requirement sheets
#define the sources of the pdfs

gen_ed_requirement_pdf_sources = {
     "https://gened.as.virginia.edu/sites/default/files/2025-04/Engagements-Pathway-Checklist.pdf": "arts_and_science_requirements.pdf",
     "https://engineering.virginia.edu/department/mechanical-and-aerospace-engineering/academics/aerospace-engineering-undergraduate-program/bs-aerospace-engineering": "gened_requirements_engineering.pdf",

}

pull_major_requirement_html_sources = {
    "https://engineering.virginia.edu/department/mechanical-and-aerospace-engineering/academics/aerospace-engineering-undergraduate-program/bs-aerospace-engineering": "major_requirements_AE.html",
    "https://engineering.virginia.edu/department/biomedical-engineering/academics/undergraduate-programs/bs-biomedical-engineering": "major_requirements_BS_BME.html",
    "https://engineering.virginia.edu/department/chemical-engineering/academics/undergraduate-programs/bs-chemical-engineering": "major_requirements_CHE.html",
    "https://engineering.virginia.edu/department/civil-and-environmental-engineering/academics/undergraduate-programs/bs-civil-engineering": "major_requirements_CE.html",
    "https://engineering.virginia.edu/offices-programs/computer-engineering-program/academics/undergraduate-program/bs-computer-engineering": "major_requirements_CpE.html",
    "https://engineering.virginia.edu/department/computer-science/academics/undergraduate-programs/bs-computer-science": "major_requirements_CS_BS.html",
    "https://engineering.virginia.edu/department/electrical-and-computer-engineering/academics/undergraduate-programs/bs-electrical-engineering": "major_requirements_EE.html",
    "https://engineering.virginia.edu/undergraduate-study/future-undergrads/special-academic-programs/engineering-science": "major_requirements_ENGS.html",
    "https://engineering.virginia.edu/department/materials-science-and-engineering/academics/undergraduate-programs/bs-materials-science-and-engineering": "major_requirements_MSE.html",
    "https://engineering.virginia.edu/department/mechanical-and-aerospace-engineering/academics/mechanical-engineering-undergraduate-program/bs-mechanical-engineering": "major_requirements_ME.html",
    "https://engineering.virginia.edu/department/systems-and-information-engineering/academics/undergraduate-programs/prospective-undergrads": "major_requirements_SIE.html",
    "https://college.as.virginia.edu/majors-college-arts-sciences": "major_requirements_college_arts_sciences.html"

}   
pull_minor_requirement_html_sources = {
     "https://engineering.virginia.edu/offices-programs/applied-mathematics/academics": "minor_requirements_applied_mathematics.html",
     "https://engineering.virginia.edu/department/biomedical-engineering/academics/undergraduate-programs/minor-biomedical-engineering": "minor_requirements_biomedical_engineering.html",
     "https://engineering.virginia.edu/department/chemical-engineering/academics/undergraduate-program/minor-chemical-engineering": "minor_requirements_chemical_engineering.html",
     "https://engineering.virginia.edu/department/civil-and-environmental-engineering/academics/undergraduate-programs/minor-civil-engineering": "minor_requirements_civil_engineering.html",
     "https://engineering.virginia.edu/department/computer-science/academics/undergraduate-programs/minor-computer-science": "minor_requirements_computer_science_bs.html",
     "https://engineering.virginia.edu/department/electrical-and-computer-engineering/academics/undergraduate-programs/minor-electrical-engineering": "minor_requirements_electrical_engineering.html",
     "https://engineering.virginia.edu/undergraduate-study/future-undergrads/special-academic-programs/business-and-entrepreneurship-programs": "minor_requirements_business_engineering.html",
     "https://engineering.virginia.edu/department/engineering-and-society/academics/history-science-and-technology-minor": "minor_requirements_history_science_and_technology.html",
     "https://engineering.virginia.edu/department/materials-science-and-engineering/academics/undergraduate-programs/minor-materials-science-and-engineering": "minor_requirements_materials_science_and_engineering.html",
     "https://engineering.virginia.edu/department/engineering-and-society/academics/e-s-undergraduate-minors/science-and-technology-policy-minor": "minor_requirements_science_and_technology_policy.html",
     "https://engineering.virginia.edu/department/engineering-and-society/academics/sts-minor": "minor_requirements_science_technology_and_society.html",
     "https://engineering.virginia.edu/department/systems-and-information-engineering/academics/undergraduate-program/minor-systems-engineering": "minor_requirements_systems_engineering.html",
     "https://engineering.virginia.edu/department/engineering-and-society/academics/e-s-undergraduate-minors/technology-ethics-minor": "minor_requirements_technology_ethics.html", 
     "https://engineering.virginia.edu/department/civil-and-environmental-engineering/academics/undergraduate-programs/technology-and-environment-minor": "minor_requirements_technology_and_environment.html",


}


def fetch_major_requirement_page_html(**kwargs):
    term = kwargs.get("term")
    fetched_pages = []
    for url, filename in pull_major_requirement_html_sources.items():
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw_html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            raw_html = None
        fetched_pages.append(
            {
                "term": term,
                "url": url,
                "filename": filename,
                "raw_html_present": raw_html is not None,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return fetched_pages

def pull_general_education_requirement_pdfs(**kwargs):

    return


def normalize_major_requirements_task(**kwargs):
    normalize_major_requirements()
    return {"status": "ok", "normalized_at": datetime.now(timezone.utc).isoformat()}


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
