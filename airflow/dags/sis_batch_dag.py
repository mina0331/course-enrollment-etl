
from airflow.providers.standard.operators.python import PythonOperator

from airflow.sdk import DAG
import requests
import json
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
import os
from airflow.models import Variable
import psycopg
from psycopg.rows import dict_row




def pull_sis_api_to_raw_data(**kwargs):
    print(">>> START pull_sis_api_to_raw_data <<<")
    term = kwargs.get('given_term')
    if not term:
        raise ValueError("A 'given_term' argument must be provided.")
    
    API_URL = f"https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term={term}&acad_career=UGRD"
    #Getting the current semester's courses from the UVA SIS API for undergraduate students
    try: 
        response = requests.get(API_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Error fetching data from SIS API: {e}")
    
    course_data = response.json()  # Parse the JSON response
    courses = course_data["results"] if isinstance(course_data, dict) and "results" in course_data else course_data
    # extracting the courses from the response, checking if the response is a dictionary and has a "results" key
    if courses is None: 
        courses = []
    if not isinstance(courses, list):
        #if the courses are not a list, convert it to a list
        courses = [courses]
    if len(courses) == 1 and isinstance(courses[0], dict) and "classes" in courses[0]:
        courses = courses[0]["classes"] or []
        #upwrapping by the key classes 
    print("First course keys:", list(courses[0].keys())[:25] if courses else "NO COURSES")
    return upsert_to_mongo(courses, term)
    

def upsert_to_mongo(courses: list[dict], term) -> None:
    MONGODB_URI = Variable.get("MONGODB_URI")
    client = MongoClient(MONGODB_URI)
    col = client["sis_raw"]["courses"]

    ingested_at = datetime.now(timezone.utc)

    operations = []
    for c in courses:
        c["ingested_at"] = ingested_at
        c["term"] = term

        class_nbr = c.get("class_nbr")
        if not class_nbr:
            print("SKIP: missing class_nbr. Keys:", list(c.keys())[:20])
            continue
        ky = {"term": term, "class_nbr": class_nbr}

        operations.append(
            UpdateOne(
                ky, 
                {"$set": c},
                upsert=True,
            )
        )
    print("Mongo operations prepared:", len(operations))
    if operations:
        result = col.bulk_write(operations, ordered=False)
        print("Mongo write -> upserted:", result.upserted_count,
          "modified:", result.modified_count,
          "matched:", result.matched_count)
    else:
        print("No operations to write")
    client.close()

def extract_transform_course_table(doc: dict) -> dict:
    return {
        "course_id": doc["crse_id"],
        "title":doc["descr"],
        "credits": doc["units"],
        "course_mnemonic": doc["subject"] + doc["catalog_nbr"],
        "section_type": doc["section_type"]
    }

def connect_mongo():
    mongo = MongoClient(Variable.get("MONGO_URI"))
    col = mongo["sis_raw"]["courses"]
    #finding the database named sis_raw and finding collection named courses 

    with psycopg.connect(Variable.get("POSTGRES_DSN")) as conn:
        with conn.cursor() as cur:
            batch = []
            for doc in col.find({}, no_cursor_timeout=True).batch_size(2000):
                row = extract_transform_course_table(doc)
                batch.append(row)

                if len(batch) >= 5000:
                    load_batch(cur, batch)
                    batch.clear()
            if batch:
                load_batch(cur, batch)

        conn.commit()

def load_batch(cur, rows):
    cur.executemany(
        """
        INSERT INTO courses(course_id, title, section_type, credits, course_mnemonic, raw_payload)
        VALUES(%(course_id)s, %(title)s, %(section_type)s, %(credits)s, %(course_mnemonic)s, %(raw_payload)s:: jsonb)
        ON CONFLICT DO NOTHING;
    """,
    rows
    )



default_args = {
    "depends_on_past": False,
    #each task instance does not wait for the same task from the previous schedule run to succeed 
    #since the course avalibility of courses changes every semester, we don't need to wait for the previous run to complete
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2026, 3, 11),
    #making sure that sis api for this semester will not run after the end of withdrawl deadline

}

#will get passed to each operator
with DAG (
    "pull_sis_api_to_raw_data",
    default_args=default_args,
    description="pull_sis_api_to_raw_data",
    schedule=timedelta(days=1),
    #run this dag every day
    start_date=datetime(2026, 1, 1),
    #start running this dag on the first day of the current semester
    catchup=False,
    
) as dag:    
    #ending instantiate_dag

    #defining the bash command to run the python script that will pull the data from the sis api and save it to the raw_data 
    t1 = PythonOperator(
        task_id="pull_sis_api_to_raw_data",
        python_callable=pull_sis_api_to_raw_data,
        retries=3,
        op_kwargs={'given_term':'1262'},
        #Spring 2026
    )

    t2 = PythonOperator(
        task_id="transforming_data",
        python_callable=connect_mongo,
        retries=3,
    )

    t1 > t2
    


