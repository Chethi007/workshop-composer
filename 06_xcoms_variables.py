#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta

from airflow import DAG
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


def function_with_context(**kwargs):
    #  Create a value for a variable defined for this task instance with a custom key.
    kwargs['task_instance'].xcom_push('custom_var_key', value='ABCDEF-' + str(datetime.now()))

    #  Create a value for a variable defined for this task instance with the default key.
    return 'XYZ-' + str(datetime.now())


with DAG(dag_id='06_xcoms_variables',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    push_values = PythonOperator(
        task_id='push_values',
        python_callable=function_with_context,
        provide_context=True
    )

    print_var_value_1 = BashOperator(
        task_id='print_var_value_1',
        bash_command='echo "Value of the default variable : {{ task_instance.xcom_pull(task_ids=\"push_values\") }}"',
    )
    push_values >> print_var_value_1

    print_var_value_2 = BashOperator(
        task_id='print_var_value_2',
        bash_command='echo "Value of the custom variable : {{ task_instance.xcom_pull(task_ids=\"push_values\", key=\"custom_var_key\") }}"'
    )
    push_values >> print_var_value_2
