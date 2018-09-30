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

with DAG(dag_id='03_task_dependencies',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    # Instanciate a task to print some text to the console.
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow !! This is the execution for date-time {{ execution_date }}"'
    )

    # Wait for some time.
    wait = BashOperator(
        task_id='wait',
        bash_command='echo "Sleeping for 10 seconds..." && sleep 10'
    )
    print_hello >> wait

    # Print end of execution.
    print_finished = BashOperator(
        task_id='print_finished',
        bash_command='echo "Execution finished !!"'
    )
    wait >> print_finished
