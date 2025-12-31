

from airflow.providers.standard.operators.python import PythonOperator

from airflow.sdk import DAG
import requests
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from airflow.models import Variable
import psycopg
from psycopg.rows import dict_row

#used to pull in the 5 years of enrollment history 

def pull_enrollment_history(**kwargs):
    print(">>> START pull_sis_api_to_raw_data <<<")
    term = 1238
    page = 1
    if not term:
        raise ValueError("A 'given_term' argument must be provided.")
    
    
    #Getting the current semester's courses from the UVA SIS API for undergraduate students
    while True:
        API_URL = f"https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term={term}&acad_career=UGRD&page={page}"
        try: 
            response = requests.get(API_URL)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Error fetching data from SIS API: {e}")
        
        course_data = response.json()  # Parse the JSON response
        courses = course_data["results"] if isinstance(course_data, dict) and "results" in course_data else course_data
        # extracting the courses from the response, checking if the response is a dictionary and has a "results" key
        if courses is None: 
            break
        if not isinstance(courses, list):
            #if the courses are not a list, convert it to a list
            courses = [courses]
        if len(courses) == 1 and isinstance(courses[0], dict) and "classes" in courses[0]:
            courses = courses[0]["classes"] or []
        if not courses:
            print("no more courses")
            break
        #upwrapping by the key classes 
        print("First course keys:", list(courses[0].keys())[:25] if courses else "NO COURSES")
        upsert_to_mongo(courses, term)
        page+=1
    

def upsert_to_mongo(courses: list[dict], term) -> None:
    MONGODB_URI = Variable.get("MONGODB_URI")
    client = MongoClient(MONGODB_URI)
    col = client["sis_raw"][f"courses_{term}"]

    ingested_at = datetime.now(timezone.utc)

    operations = []
    for c in courses:
        c["ingested_at"] = ingested_at
        c["term"] = term

        class_nbr = c.get("class_nbr")
        if not class_nbr:
            continue
        ky = {"term": term, "class_nbr": class_nbr}

        operations.append(
            UpdateOne(
                ky,
                {"$set": c},
                upsert=True,
            )
        )
    if operations:
        col.bulk_write(operations, ordered=False)
    client.close()

if __name__ == "__main__":
    pull_enrollment_history()    

