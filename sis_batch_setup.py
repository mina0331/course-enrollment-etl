
from airflow.providers.standard.operators.python import PythonOperator

from airflow.sdk import DAG
from datetime import timedelta, datetime

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
    start_date=datetime(2025, 1, 1),
    #start running this dag on the first day of the current semester
    catchup=False,
    
) as dag:    
    #ending instantiate_dag

    #defining the bash command to run the python script that will pull the data from the sis api and save it to the raw_data 
    t1 = PythonOperator(
        task_id="pull_sis_api_to_raw_data",
        python_callable=pull_sis_api_to_raw_data,
        retries=3,
        op_kwargs={"term": 1252},
    )


