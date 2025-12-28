# course-enrollment-etl
An end-to-end ETL data pipeline that ingests structured data from external APIs, applies validation and transformation logic, and loads normalized records into a PostgreSQL database, designed to be cloud-ready and easily deployable.

# Airflow Setup
export AIRFLOW_HOME=./airflow
airflow db migrate
airflow scheduler
airflow webserver


