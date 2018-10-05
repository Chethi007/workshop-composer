from datetime import datetime, timedelta
from pprint import pprint
import random
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator

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


def my_branching_function():
    r = random.randint(1, 2)
    return 'branch_1_task' if r == 1 else 'branch_2_task'


with DAG(dag_id='07_branching',
         schedule_interval='*/1 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=my_branching_function
    )

    branch_1_task = BashOperator(
        task_id='branch_1_task',
        bash_command='echo "Hello from branch 1 !!"'
    )
    branching_task >> branch_1_task

    branch_2_task = BashOperator(
        task_id='branch_2_task',
        bash_command='echo "Hello from branch 2 !!"'
    )
    branching_task >> branch_2_task

    join = DummyOperator(
        task_id='join'
    )
    branch_1_task >> join
    branch_2_task >> join
