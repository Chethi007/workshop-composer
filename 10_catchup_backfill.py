from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today() - timedelta(days=3),
    'depends_on_past': False,
    'email': ['your_mail@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(dag_id='10_catchup_backfill',
         schedule_interval='@daily',
         catchup=True,
         default_args=default_args) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Execution date-time : {{ execution_date }}"'
    )
