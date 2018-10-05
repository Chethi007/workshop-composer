from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

yesterday = datetime.combine(datetime.today() - timedelta(1),datetime.min.time())

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': ['your_mail@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(dag_id='02_hello_world_with_scheduling',
         schedule_interval='*/2 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    # Instanciate a task to print some text to the console.
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow !! This is the execution for date-time {{ execution_date }}"'
    )
