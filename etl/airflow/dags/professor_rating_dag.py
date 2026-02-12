from typing import Any
from urllib import request
from bs4 import BeautifulSoup

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, timezone



import urllib
import os
from sqlalchemy import create_engine, text



def fetching_courses_to_pull_in_for(conn) -> dict[str, list[dict[str, Any]]]:
    courses = {}
    results = conn.execute(text("SELECT subject_id, catalog_nbr, course_id FROM course;")).mappings()
    for row in results:
        subject = row["subject_id"]
        catalog_nbr = row["catalog_nbr"]
        course_id = row["course_id"]
        courses.setdefault(subject, []).append({"course_id": course_id, "catalog_nbr": catalog_nbr})
    return courses

def pull_professor_rating_raw_html(**kwargs):
    term = kwargs.get("term")
    
    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:
        courses = fetching_courses_to_pull_in_for(conn)
        rows = []
        for subject, items in courses.items(): 
            for item in items:
                catalog_nbr = item["catalog_nbr"]
                course_id = item["course_id"]
                url_course_forum = f"https://thecourseforum.com/course/{subject.upper()}/{catalog_nbr}/All"

                print("Fetching URL:", url_course_forum)
                #opening the url for reading
                req = urllib.request.Request(url_course_forum, headers={"User-Agent": "Mozilla/5.0"})
                #adding a user agent so that the access won't get blocked as easily 
                fetched_at = datetime.now(timezone.utc)
                try:
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        raw_html = resp.read().decode("utf-8", errors="replace")
                except Exception as e:
                    raw_html = ""
                
                rows.append({"term": term, "subject": subject, "catalog_nbr": catalog_nbr, "course_id": course_id, "url": url_course_forum, "raw_html": raw_html, "fetched_at": fetched_at})
                
        if rows:
            conn.execute(text("INSERT INTO professor_rating_raw_html (term, subject, catalog_nbr, course_id, url, raw_html, fetched_at) VALUES (:term, :subject, :catalog_nbr, :course_id, :url, :raw_html, :fetched_at) ON CONFLICT (subject, catalog_nbr, course_id) DO UPDATE SET raw_html = EXCLUDED.raw_html, fetched_at = EXCLUDED.fetched_at"), rows)
            
        
        

def transform_html_file_instructor(**kwargs):
    term = kwargs.get("term")
    

    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:

        courses = fetching_courses_to_pull_in_for(conn)
        for subject, items in courses.items():
            for item in items:
                catalog = item["catalog_nbr"]
                course_id = item["course_id"]
                url_course_forum = f"https://thecourseforum.com/course/{subject.upper()}/{catalog}/All"

                html_fetched_instructor_ratings = conn.execute(text("SELECT raw_html FROM professor_rating_raw_html WHERE url = :url"), {"url": url_course_forum}).mappings().all()
        
                for html_fetched_instructor_rating in html_fetched_instructor_ratings:
                    if (html_fetched_instructor_rating["raw_html"] != ""):
                        html_parsed = BeautifulSoup(html_fetched_instructor_rating["raw_html"], 'html.parser')
                        instructor_items = html_parsed.find_all("li", class_="instructor")
                        print(f"Processing course {subject} {catalog} with {len(instructor_items)} instructors")
                        for instructor in instructor_items:
                            rating = instructor.find("p", id="rating").text.strip()
                            difficulty=instructor.find("p", id="difficulty").text.strip()
                            name = instructor.find("h3", id="title").text.strip()
                            if (rating != "—"):
                                result = conn.execute(text("SELECT professor_id FROM professor WHERE name = :name"), {"name": name}).mappings().all()
                                if len(result) > 1:
                                    print(f"WARNING: Multiple professors found with name {name}. Skipping rating update for this instructor.")
                                    continue
                                if len(result) == 0:
                                    print(f"WARNING: No professor found with name {name}. Skipping rating update for this instructor.")
                                    continue
                                else:
                                    conn.execute(text("UPDATE section_professor SET rating = :rating WHERE professor_id = :professor_id and course_id = :course_id"), {"rating": rating, "professor_id": result[0]["professor_id"], "course_id": course_id})
                                    print(f"Updated instructor {name} with rating {rating}")
                                    if (difficulty != "—"):
                                        conn.execute(text("UPDATE section_professor SET difficulty = :difficulty WHERE professor_id = :professor_id and course_id = :course_id"), {"difficulty": difficulty, "professor_id": result[0]["professor_id"], "course_id": course_id})
                                        print(f"Updated instructor {name} with difficulty {difficulty}")
                
    


default_args= {
    "depends_on_past": False,
    "retries" : 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG (
    "fetching_raw_html_from_course_forum",
    default_args=default_args,
    description="fetching_raw_html_file_from_course_forum",
    schedule=timedelta(days=7),
    #run this dag every week
    start_date=datetime(2026,1,1),
    catchup=False,


) as dag:
    
    t1 = PythonOperator(
        task_id="fetching_the_raw_html_file_for_professor_rating",
        op_kwargs={'term': '1262'},
        python_callable=pull_professor_rating_raw_html,
        retries=3,
        #spring 2026
    )
    t2 = PythonOperator(
        task_id="transforming_html_file_to_get_instructor_rating",
        op_kwargs={'term': '1262'},
        python_callable=transform_html_file_instructor,
        retries=3,
        #spring 2026
    )

    t1 >> t2


#courses that we're pulling in should be for the current semester, and as the semester goes on, we should be dynamically pulling in the data for more courses? 
