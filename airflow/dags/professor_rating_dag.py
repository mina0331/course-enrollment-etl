from urllib import request
from bs4 import BeautifulSoup

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.models import Variable

from pymongo import MongoClient, UpdateOne
import psycopg
import urllib



def fetching_courses_to_pull_in_for():
    courses = {}
    POSTGRES_DSN = Variable.get("POSTGRES_DSN")
    
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT subject, catalog_nbr, course_id FROM courses;")
            for subject, catalog_nbr, course_id in cur:
                courses.setdefault(subject, []).append({"course_id": course_id, "catalog_nbr": catalog_nbr})
    conn.close()
    return courses

def pull_professor_rating_raw_html(**kwargs):
    MONGODB_URI = Variable.get("MONGODB_URI")
    client = MongoClient(MONGODB_URI)
    col = client["course_forum_raw"]["professor_rating"]
    term = kwargs.get("term")
    courses = fetching_courses_to_pull_in_for()
    bulk_op = []
    for subject, items in courses.items(): 
        for item in items:
            catalog_nbr = item["catalog_nbr"]
            course_id = item["course_id"]
            url_course_forum = f"https://thecourseforum.com/course/{subject.upper()}/{catalog_nbr}/All"
            print("Fetching URL:", url_course_forum)
            #opening the url for reading
            req = urllib.request.Request(url_course_forum, headers={"User-Agent": "Mozilla/5.0"})
            #adding a user agent so that the access won't get blocked as easily 

            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    raw_html = resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                raw_html = None
            
            doc_filter = {
                "term": term,
                "subject": subject,
                "catalog_nbr": catalog_nbr,
                "url": url_course_forum,
            }
            #used to locate a document: to prevent duplicates 

            doc_update = {
                "$set": {
                    "term": term,
                    "subject": subject,
                    "catalog_nbr": catalog_nbr,
                    "course_id": course_id,
                    "url": url_course_forum,
                    "raw_html": raw_html,
                    "fetched_at": datetime.now(timezone.utc)

                }   
            }
            #used to define what the document is supposed to be strucutred 

            bulk_op.append(UpdateOne(doc_filter, doc_update, upsert=True))
            #dumping the html file onto the mongo_db 
    if bulk_op:
        col.bulk_write(bulk_op, ordered=False)
    client.close()
    

def transform_html_file_instructor(**kwargs):
    term = kwargs.get("term")
    courses = fetching_courses_to_pull_in_for()
    MONGODB_URI = Variable.get("MONGODB_URI")
    client = MongoClient(MONGODB_URI)
    col = client["course_forum_raw"]["professor_rating"]
    #accessing the raw html files already fetched and stored inside the mongodb 
    
    with psycopg.connect(Variable.get("POSTGRES_DSN")) as conn: 
        with conn.cursor() as cur:
            for subject, items in courses.items():
                for item in items:
                    catalog = item["catalog_nbr"]
                    course_id = item["course_id"]
                    url_course_forum = f"https://thecourseforum.com/course/{subject.upper()}/{catalog}/All"
                    doc_filter={
                        "term": term,
                        "subject": subject,
                        "catalog_nbr": catalog,
                        "url": url_course_forum,
                    }

                    html_fetched_instructor_ratings = col.find(doc_filter)
                    print(f"Found {html_fetched_instructor_ratings.count()} HTML documents for course {subject} {catalog}")
                    
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
                                    cur.execute("UPDATE instructors SET rating = %s WHERE name = %s AND course_id=%s;", (rating, name, course_id))
                                    print(f"Updated instructor {name} with rating {rating} for course_id {course_id}")
                                if (difficulty != "—"):
                                    cur.execute("UPDATE instructors SET difficulty = %s WHERE name = %s AND course_id=%s;", (difficulty,name, course_id))
                                    print(f"Updated instructor {name} with difficulty {difficulty} for course_id {course_id}")
                conn.commit()
    conn.close()
    client.close()


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
