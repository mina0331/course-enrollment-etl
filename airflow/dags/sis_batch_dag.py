
from airflow.providers.standard.operators.python import PythonOperator

from airflow.sdk import DAG
import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
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
        "credits": int(doc["units"]) if "units" in doc and str(doc["units"]).isdigit() else None,
        "course_mnemonic": doc["subject"] + doc["catalog_nbr"],
        "has_discussion": doc.get("component") == "DISC",
        "has_lab": doc.get("component") == "LAB",

    }

def extract_transform_section_table(doc: dict) -> dict:
    meetings = doc.get("meetings", [])
    meeting_info = meetings[0] if meetings else {}
    return {
        "section_id": doc["class_nbr"],
        "course_id": doc["crse_id"],
        "term": doc["term"],
        "instructor_name": doc.get("instructors", [{}])[0].get("name"),
        "capacity": int(doc.get("class_capacity", 0)),
        "enrollment_status": doc.get("enrl_stat"),
        "seats_taken": int(doc.get("enroll_total", 0)),
        "waitlist_size": doc.get("wait_cap", 0),
        "current_waitlist": doc.get("wait_tot", 0),
        "meetings_days": meeting_info.get("days"),
        "meetings_start_time": meeting_info.get("start_time"),
        "meetings_end_time": meeting_info.get("end_time"),
    }

def extract_transform_instrucotr_table(doc: dict) -> dict:
    instructor_info = doc.get("instructors", [{}])
    instructors = []
    for instructor in instructor_info:
        if instructor.get("name") == "To Be Announced":
            continue
        instructors.append({
            "name": instructor.get("name"),
            "email": instructor.get("email"),
        })
    return instructors

def connect_mongo(**kwargs):
    table_name = kwargs.get('table_name')
    mongo = MongoClient(Variable.get("MONGO_URI"))
    col = mongo["sis_raw"]["courses"]
    #finding the database named sis_raw and finding a collection named courses 

    with psycopg.connect(Variable.get("POSTGRES_DSN")) as conn:
        with conn.cursor() as cur:
            batch = []
            for doc in col.find({}, no_cursor_timeout=True).batch_size(2000):
                if (table_name == 'courses'):
                    row = extract_transform_course_table(doc)
                if (table_name == 'sections'):
                    row = extract_transform_section_table(doc)
                if (table_name == 'instructors'):
                    instructors = extract_transform_instrucotr_table(doc)
                    for instructor in instructors:
                        batch.append(instructor)
                    continue
                batch.append(row)

                if len(batch) >= 5000:
                    if table_name == 'courses':
                        load_batch_courses(cur, batch)
                        batch.clear()
                    elif table_name == 'sections':
                        load_batch_sections(cur, batch)
                        batch.clear()
                    elif table_name == 'instructors':
                        load_batch_instructors(cur, batch)
                        batch.clear()
            if batch:
                if table_name == 'courses':
                    load_batch_courses(cur, batch)
                elif table_name == 'sections':
                    load_batch_sections(cur, batch)

        conn.commit()

def load_batch_courses(cur, rows):
    cur.executemany(
        """
        INSERT INTO courses(course_id, title, credits, course_mnemonic, has_discussion, has_lab)
        VALUES(%(course_id)s, %(title)s, %(credits)s, %(course_mnemonic)s, %(has_discussion)s, %(has_lab)s)
        ON CONFLICT (course_mnemonic) DO UPDATE SET
            has_discussion = EXCLUDED.has_discussion or courses.has_discussion,
            has_lab = EXCLUDED.has_lab or courses.has_lab,
    """,
    rows
    )

def load_batch_sections(cur, rows):
    cur.executemany(
        """
        INSERT INTO sections(section_id, course_id, term, instructor_name, capacity, enrollment_status, seats_taken, waitlist_size, current_waitlist, meetings_days, meetings_start_time, meetings_end_time)
        VALUES(%(section_id)s, %(course_id)s, %(term)s, %(instructor_name)s, %(capacity)s, %(enrollment_status)s, %(seats_taken)s, %(waitlist_size)s, %(current_waitlist)s, %(meetings_days)s, %(meetings_start_time)s, %(meetings_end_time)s)
        ON CONFLICT DO NOTHING;
    """,
    rows
    )

def load_batch_instructors(cur, rows):
    cur.executemany(
        """
        INSERT INTO instructors(name, email)
        VALUES(%(name)s, %(email)s)
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
    schedule=timedelta(days=7),
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
        task_id="transforming_data_course",
        python_callable=connect_mongo,
        retries=3,
        op_kwargs={'table_name':'courses'},
    )

    t3 = PythonOperator(
        task_id="transforming_data_section",
        python_callable=connect_mongo,
        retries=3,
        op_kwargs={'table_name':'sections'},
    )
    t4 = PythonOperator(
        task_id="transforming_data_instructor",
        python_callable=connect_mongo,
        retries=3,
        op_kwargs={'table_name':'instructors'},

    )
       

    t1 > t2 > t3
    


