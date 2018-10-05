from datetime import datetime, timedelta
from pprint import pprint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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


def function_with_no_args():
    print('Hello Airflow from Python !!')


def function_with_args(name_to_print):
    print('Hello {who} from Python !!'.format(who=name_to_print))


def function_with_context(**kwargs):
    print('Execution context:')
    pprint(kwargs)
    print('Hello from Python, the execution date-time is {}'.format(kwargs['execution_date']))


with DAG(dag_id='04_python_operator',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    # Instanciate a task to print some text to the console.
    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=function_with_no_args
    )

    print_hello_with_positional_args = PythonOperator(
        task_id='print_hello_with_positional_args',
        python_callable=function_with_args,
        op_args=['toto']
    )

    print_hello_with_kw_args = PythonOperator(
        task_id='print_hello_with_kw_args',
        python_callable=function_with_args,
        op_kwargs={'name_to_print': 'titi'}
    )

    print_hello_with_context = PythonOperator(
        task_id='print_hello_with_context',
        python_callable=function_with_context,
        provide_context=True
    )
