from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

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


def create_subdag_1(parent_dag_id, subdag_name, schedule_interval):

    def hello():
        print('Hello')

    def world():
        print('World!')

    with DAG(dag_id='{}.{}'.format(parent_dag_id, subdag_name),
             schedule_interval=schedule_interval,
             catchup=False,
             default_args=default_args) as subdag:
        hello = PythonOperator(
            task_id='hello',
            python_callable=hello
        )

        world = PythonOperator(
            task_id='world',
            python_callable=world
        )
        hello >> world

        return subdag


def create_subdag_2(parent_dag_id, subdag_name, schedule_interval):
    with DAG(dag_id='{}.{}'.format(parent_dag_id, subdag_name),
             schedule_interval=schedule_interval,
             catchup=False,
             default_args=default_args) as subdag:
        task = BashOperator(
            task_id='task',
            bash_command='echo "Sub-DAG 2 executed !!"'
        )

        return subdag


with DAG(dag_id='08_subdags',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    sub_dag_1_name = 'sub_dag_1'
    sub_dag_1_task = SubDagOperator(
        subdag=create_subdag_1(dag.dag_id, sub_dag_1_name, dag.schedule_interval),
        task_id=sub_dag_1_name
    )

    foo = DummyOperator(
        task_id='foo'
    )
    sub_dag_1_task >> foo

    sub_dag_2_name = 'sub_dag_2'
    sub_dag_2_task = SubDagOperator(
        subdag=create_subdag_2(dag.dag_id, sub_dag_2_name, dag.schedule_interval),
        task_id=sub_dag_2_name
    )
    foo >> sub_dag_2_task
