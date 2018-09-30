from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 10, 8),
    'depends_on_past': False,
    'email': ['your_mail@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(dag_id='01_hello_world_without_scheduling',
         schedule_interval=None,
         catchup=False,
         default_args=default_args) as dag:
    # Instanciate a task to print some text to the console.
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow !!"'
    )
