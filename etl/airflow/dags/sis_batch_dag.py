
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG
import requests
from datetime import datetime, timezone, timedelta


import json 
from sqlalchemy import create_engine, text
import os 


def iter_docs_paged(col, page_size=2000, query=None, projection=None):
    query = query or {}
    last_id = None

    # ensure _id is included for paging
    if projection is not None:
        projection = dict(projection)
        projection["_id"] = 1

    while True:
        q = dict(query)
        if last_id is not None:
            q["_id"] = {"$gt": last_id}

        page = list(
            col.find(q, projection).sort("_id", 1).limit(page_size)
        )
        if not page:
            break

        for doc in page:
            yield doc

        last_id = page[-1]["_id"]


def pull_sis_api_to_raw_data(**kwargs):
    print(">>> START pull_sis_api_to_raw_data <<<")
    term = kwargs.get('given_term')
    page = 1
    
    if not term:
        raise ValueError("A 'given_term' argument must be provided.")
    

    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:
        while True:
            API_URL = f"https://sisuva.admin.virginia.edu/psc/ihprd/UVSS/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UVA01&term={term}&acad_career=UGRD&page={page}"
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
                break
            if not isinstance(courses, list):
                #if the courses are not a list, convert it to a list
                courses = [courses]
            if len(courses) == 1 and isinstance(courses[0], dict) and "classes" in courses[0]:
                courses = courses[0]["classes"] or []
            if not courses:
                break
            #upwrapping by the key classes 
            print("First course keys:", list(courses[0].keys())[:25] if courses else "NO COURSES")
            
            upsert_to_database(
                {"term": term, "page": page, "classes": courses},
                term=term,
                page=page,
                conn=conn,
            )
            page+=1
        
        

def upsert_to_database(courses: list[dict], term, page, conn) -> None:
    ingested_at = datetime.now(timezone.utc)
    classes = courses["classes"] if isinstance(courses, dict) else courses

    rows = []
    for c in classes:
        c["term"] = term
        c["page"] = page
        c["fetched_at"] = ingested_at
        class_nbr = c.get("class_nbr")
        if not class_nbr:
            print("SKIP: missing class_nbr. Keys:", list(c.keys())[:20])
            continue
        payload = json.dumps(c, separators=(",", ":"), default=str)  # Convert to JSON string for storage

        rows.append(
            {"term": term, 
             "class_nbr": str(class_nbr), 
             "payload": payload, 
             "page_fetched": int(page), 
             "fetched_at": ingested_at}
        )
    print("Operations prepared:", len(rows))
    if rows:
        conn.execute(text("""
            INSERT INTO raw_sis_data(
                term_id,
                class_nbr,
                payload,
                page_fetched,
                fetched_at
            )
            VALUES (:term, :class_nbr, CAST(:payload AS jsonb), :page_fetched, :fetched_at)
            ON CONFLICT (term_id, class_nbr) DO UPDATE SET
                page_fetched = EXCLUDED.page_fetched,
                payload = EXCLUDED.payload,
                fetched_at = EXCLUDED.fetched_at
            """)
            ,
            rows,
        )
    else:
        print("No operations to write")
    


def extract_transform_course_table(doc: dict) -> dict:
    return {
        "course_id": doc["crse_id"],
        "title":doc["descr"],
        "credits": int(doc["units"]) if "units" in doc and str(doc["units"]).isdigit() else None,
        "subject_id": doc["subject"],
        "catalog_nbr": doc["catalog_nbr"],
        "has_discussion": doc.get("component") == "DIS",
        "has_lab": doc.get("component") == "LAB",
        "component": doc.get("component"),

    }

def extract_transform_section_table(doc: dict, term) -> dict:
    meetings = doc.get("meetings", [])
    meeting_info = meetings[0] if meetings else {}
    
    return {
        "course_id": doc["crse_id"],
        "class_nbr": doc["class_nbr"],
        "term_id": term,
        "capacity": doc.get("class_capacity", 0),
        "enrollment_status": doc.get("enrl_stat"),
        "seats_taken": doc.get("enrollment_total", 0),
        "waitlist_size": doc.get("wait_cap", 0),
        "current_waitlist": doc.get("wait_tot", 0),
        "meetings_days": meeting_info.get("days", "TBA"),
        "meetings_start_time": meeting_info.get("start_time", "TBA"),
        "meetings_end_time": meeting_info.get("end_time", "TBA"),
        
    }

def extract_transform_instructor_table(doc: dict, term) -> dict:
    instructor_info = doc.get("instructors", [{}])
    instructors = []
    for instructor in instructor_info:
        name = instructor.get("name")
        if name in {"To Be Announced", "-"}:
            print("SKIP: To Be Announced instructor for course_id:", doc["crse_id"])
            continue
        print("Processing instructor:", instructor.get("name"), "for course_id:", doc["crse_id"])
        instructors.append({
            "name": instructor.get("name"),
            "email": instructor.get("email"),
            "course_id": doc["crse_id"],
            "term_id": term,
            
        })
    return instructors

def connect_database(**kwargs):
    table_name = kwargs.get('table_name')
    term = kwargs.get('given_term')
    
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    with engine.begin() as conn:
        try: 
            
            result = conn.execute(text("""
                SELECT payload FROM raw_sis_data WHERE term_id = :term
            """), {"term": term})
            
            for row in result.mappings():
                
                payload = row["payload"]
            
                #tunring raw json to dict 
                if table_name == "course":
                    course_doc = extract_transform_course_table(payload)
                    flush(conn, table_name, [course_doc])
                elif table_name == "section":
                    section_doc = extract_transform_section_table(payload, term)
                    flush(conn, table_name, [section_doc])
                elif table_name == "professor":    
                    instructor_docs = extract_transform_instructor_table(payload, term)
                    flush(conn, table_name, instructor_docs)
                else:
                    raise ValueError(f"Unknown table name: {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"Error processing {table_name} for term {term}: {e}")
            raise


    
    

def flush(conn, table_name, batch):
    try:
        if table_name == "course":
            load_batch_courses(conn, batch)  # make this return inserted count if possible
            
        elif table_name == "section":
            load_batch_sections(conn, batch)
        else:
            load_batch_instructors(conn, batch)
        batch.clear()
    except Exception as e:
        conn.rollback()
        print(f"FLUSH FAILED {table_name}: {e}")
        # optionally: print the first bad row
        print("sample row:", batch[0] if batch else None)
        raise

    
        

def load_batch_courses(conn, rows):
    conn.execute(text("""
        INSERT INTO course(
            course_id,
            title,
        credits,
        subject_id,
        catalog_nbr,
        has_discussion,
        has_lab,
        component
    )
    VALUES (
        :course_id,
        :title,
        :credits,
        :subject_id,
        :catalog_nbr,
        :has_discussion,
        :has_lab,
        :component
    )
    ON CONFLICT(course_id) DO UPDATE SET
        has_discussion = excluded.has_discussion OR course.has_discussion,
        has_lab       = excluded.has_lab       OR course.has_lab
    """),
        rows
    )
    
    

def load_batch_sections(conn, rows):
    conn.execute(text(
        """
        INSERT INTO section(
            course_id,
            class_nbr,
            term_id,
            capacity,
            enrollment_status,
            seats_taken,
            waitlist_size,
            current_waitlist,
            meetings_days,
            meetings_start_time,
            meetings_end_time
        )
        VALUES (
            :course_id,
            :class_nbr,
            :term_id,
            :capacity,
            :enrollment_status,
            :seats_taken,
            :waitlist_size,
            :current_waitlist,
            :meetings_days,
            :meetings_start_time,
            :meetings_end_time
        )
        ON CONFLICT (class_nbr, term_id) DO UPDATE SET
            course_id = EXCLUDED.course_id,
            meetings_days = EXCLUDED.meetings_days,
            meetings_start_time = EXCLUDED.meetings_start_time,
            meetings_end_time = EXCLUDED.meetings_end_time
        """),
        rows
    )

def load_batch_instructors(conn, rows):
    print("Loading instructors, count:", len(rows))
    for row in rows:
        name = row["name"]
        email = row["email"]
        conn.execute(text(
                """
                INSERT INTO professor (name, email)
            VALUES (:name, :email)
            ON CONFLICT (name, email) DO NOTHING
            """),
            {"name": name, "email": email}
        )
        
        prof_id = conn.execute(text("SELECT professor_id FROM professor WHERE name = :name AND email = :email"), {"name": name, "email": email}).scalar_one_or_none()

        if prof_id is not None:
            conn.execute(text(
                """
                INSERT INTO section_professor(
                    term_id,
                    course_id,
                    professor_id
                ) VALUES (:term_id, :course_id, :professor_id)
                """ ),
                {
                    "term_id": row["term_id"],
                    "course_id": row["course_id"],
                    "professor_id": prof_id
                }
            )
        else:
            print(f"WARNING: Could not find professor_id for {name}, {email}")
    


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
    #run this dag every week
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
        python_callable=connect_database,
        retries=3,
        op_kwargs={'table_name':'course', 'given_term': '1262'},
    )

    t3 = PythonOperator(
        task_id="transforming_data_section",
        python_callable=connect_database,
        retries=3,
        op_kwargs={'table_name':'section', 'given_term': '1262'},
    )
    t4 = PythonOperator(
        task_id="transforming_data_instructor",
        python_callable=connect_database,
        retries=3,
        op_kwargs={'table_name':'professor', 'given_term': '1262'},

    )
    

    t1 >> t2 >> t3 >> t4
    


