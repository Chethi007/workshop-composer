from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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

def log():
    print('Variable value : {}'.format(Variable.get('my_variable')))

with DAG(dag_id='05_static_variables',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    # Instanciate a task to print some text to the console.
    print_var_value_with_bash = BashOperator(
        task_id='print_var_value_with_bash',
        bash_command='echo "Here is the value of the static variable : {{ var.value.my_variable }}"'
    )

    print_var_value_with_python = PythonOperator(
        task_id='print_var_value_with_python',
        python_callable=log,
        provide_context=True
    )
