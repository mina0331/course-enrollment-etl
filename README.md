# course-enrollment-etl

This project builds an end-to-end ETL pipeline that ingests university course enrollment data,
normalizes it into a relational schema, and prepares it for downstream analytics and modeling.

The pipeline extracts raw course, section, and instructor data from term-based sources, handles
inconsistent schemas across semesters, and transforms nested JSON into clean, queryable tables.
Special care is taken to support incremental loads, idempotent upserts, and schema drift between terms.

Data is stored in a relational database with structured tables for courses, sections, instructors,
and term metadata, while preserving raw payloads for traceability. The pipeline is designed to be
reproducible, debuggable, and extensible for future features such as waitlist outcome prediction
and enrollment forecasting.

Key challenges addressed include handling mixed JSON formats, resolving entity relationships across
terms, and ensuring consistent identifiers despite changes in upstream data.

This project serves as the data foundation for course enrollment analytics, historical trend analysis,
and predictive modeling.

---

## Tech Stack

### Languages
- Python

### Data Engineering
- Apache Airflow for orchestration
- ETL pipelines with batch and incremental loads
- JSON normalization and schema drift handling

### Databases
- PostgreSQL for relational storage
- SQLite for local development and testing
- JSON / JSONB for raw payload preservation

### Data Modeling
- Normalized relational schema
- Idempotent upserts and conflict resolution
- Foreign key relationships across courses, sections, instructors, and terms

### Infrastructure & Tooling
- Docker and docker-compose for reproducible environments
- Git and GitHub for version control
- Command-line tooling for local debugging and validation

### Future Extensions
- Enrollment and waitlist outcome prediction
- Feature engineering for course demand modeling
- Student-facing analytics and decision support tools

---

## How To Run

### 1. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the local Postgres + Airflow stack

From the repo root:

```bash
docker compose -f airflow-docker/docker-compose.yaml up -d
```

The application database is exposed at:
- `postgresql+psycopg2://app:app@localhost:5433/appdb`

### 3. Populate or refresh the database

If you are moving local SQLite data into Postgres:

```bash
python migration.py
```

If you want Airflow to run the ETL pipelines, open:
- Airflow API/UI stack: [http://localhost:8080](http://localhost:8080)

Then trigger the relevant DAGs, such as:
- SIS ingestion DAGs
- Course Forum review DAG
- major requirement DAG

### 4. Run the Flask dashboard

```bash
export DATABASE_URL=postgresql+psycopg2://app:app@localhost:5433/appdb
venv/bin/flask --app app.main run
```

Then open:
- [http://127.0.0.1:5000](http://127.0.0.1:5000)

### 5. Run model evaluation

To evaluate the current section fill model locally:

```bash
export DATABASE_URL=postgresql+psycopg2://app:app@localhost:5433/appdb
venv/bin/python prediction_model/train_section_baseline.py
```

### 6. Generate synthetic student-section training data

```bash
export DATABASE_URL=postgresql+psycopg2://app:app@localhost:5433/appdb
venv/bin/python prediction_model/student_training_data.py
```

This writes:
- [prediction_model/student_section_training_data.csv](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/student_section_training_data.csv)

### Notes

- The dashboard currently reads live data from Postgres, not SQLite.
- The Course Forum review and sentiment features depend on the review-ingestion Airflow DAG having run successfully.
- If Docker is not running, the dashboard and model scripts will not be able to reach the local Postgres database on port `5433`.

---

## Project Notes

- Synthetic student-section dataset notes:
  [docs/synthetic_training_data.md](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/docs/synthetic_training_data.md)
- Model accuracy improvement report for April 5, 2026:
  [docs/model_accuracy_improvement_2026-04-05.md](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/docs/model_accuracy_improvement_2026-04-05.md)
