
import requests
import json
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import os
from airflow.models import Variable



def pull_sis_api_to_raw_data(**kwargs):
    term = kwargs.get('given_term')
    if not term:
        raise ValueError("A 'given_term' argument must be provided.")
    
    API_URL = f"https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term={term}&acad_career=UGRD"
    #Getting the current semester's courses from the UVA SIS API for undergraduate students
    try: 
        response = requests.get(API_URL, timeout=60)
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

        ky = {"term": term, "class_nbr": c.get("class_nbr")}
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



    



    
    
    