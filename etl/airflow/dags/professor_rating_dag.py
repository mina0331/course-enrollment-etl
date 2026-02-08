from typing import Any
from urllib import request
from bs4 import BeautifulSoup

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.models import Variable

from pymongo import MongoClient, UpdateOne
import psycopg
import urllib
import sqlite3


def fetching_courses_to_pull_in_for(conn) -> dict[str, list[dict[str, Any]]]:
    courses = {}
    with conn.cursor() as cur:
        cur.execute("SELECT subject_id, catalog_nbr, course_id FROM course;")
        for subject, catalog_nbr, course_id in cur:
            courses.setdefault(subject, []).append({"course_id": course_id, "catalog_nbr": catalog_nbr})
    return courses

def pull_professor_rating_raw_html(**kwargs):
    term = kwargs.get("term")
    conn = sqlite3.connect("data/app.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 50000;")
    
    conn.execute("BEGIN")
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
                raw_html = None
            
            
            
            rows.append((term, subject, catalog_nbr, course_id, url_course_forum, raw_html,fetched_at))
            

    
    if rows:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO professor_rating_raw_html (term, subject, catalog_nbr, course_id, url, raw_html, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        conn.commit()
    conn.close()
    

def transform_html_file_instructor(**kwargs):
    term = kwargs.get("term")

    conn = sqlite3.connect("data/app.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("BEGIN")

    courses = fetching_courses_to_pull_in_for(conn)
    
    
    with conn.cursor() as cur:
        for subject, items in courses.items():
            for item in items:
                catalog = item["catalog_nbr"]
                course_id = item["course_id"]
                url_course_forum = f"https://thecourseforum.com/course/{subject.upper()}/{catalog}/All"

                html_fetched_instructor_ratings = cur.execute("SELECT raw_html FROM professor_rating_raw_html WHERE url = ?", (url_course_forum,)).fetchall()
    
                for html_fetched_instructor_rating in html_fetched_instructor_ratings:
                    if (html_fetched_instructor_rating["raw_html"]):
                        html_parsed = BeautifulSoup(html_fetched_instructor_rating["raw_html"], 'html.parser')
                        instructor_items = html_parsed.find_all("li", class_="instructor")
                        print(f"Processing course {subject} {catalog} with {len(instructor_items)} instructors")
                        for instructor in instructor_items:
                            rating = instructor.find("p", id="rating").text.strip()
                            difficulty=instructor.find("p", id="difficulty").text.strip()
                            name = instructor.find("h3", id="title").text.strip()
                            if (rating != "—"):
                                professor_id = cur.execute("SELECT professor_id FROM professor WHERE name = ?", (name,)).fetchone()
                                if professor_id:
                                    cur.execute("UPDATE section_professor SET rating = ? WHERE professor_id = ? AND course_id=?;", (rating, professor_id["professor_id"], course_id))
                                    print(f"Updated instructor {name} with rating {rating} for course_id {course_id}")
                            if (difficulty != "—"):
                                cur.execute("UPDATE section_professor SET difficulty = ? WHERE professor_id = ? AND course_id=?;", (difficulty,professor_id["professor_id"], course_id))
                                print(f"Updated instructor {name} with difficulty {difficulty} for course_id {course_id}")
            conn.commit()
    conn.close()
    


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
