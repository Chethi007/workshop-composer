import os
from datetime import datetime, timedelta
import csv

from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def get_composer_gcs_bucket():
    return os.environ['GCS_BUCKET']


def sql_import(**kwargs):
    input_file = kwargs['templates_dict']['input_file']
    columns = ["CODE", "NUMBER_RELATED_ORDERS", "NUMBER_STATUSES", "NUMBER_PARTNERS", "NUMBER_COMMENTS"]
    mysql = MySqlHook(mysql_conn_id='workshop_sql_conn_id')
    mysql.run("TRUNCATE PROCESSED_ORDER")
    with open(input_file) as file:
        reader = csv.reader(file, delimiter=' ')
        data = list(reader)
        mysql.insert_rows('PROCESSED_ORDER', data, target_fields=columns)


with DAG(dag_id='31_scenario',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:

    copy_file_to_gcs = FileToGoogleCloudStorageOperator(
        task_id='copy_file_to_gcs',
        bucket='{{ get_composer_gcs_bucket() }}',
        dst='data/31_scenario/{{ execution_date }}/order.json',
        src='/home/airflow/gcs/data/31_scenario/31_order.json',
        mime_type='application/json'
    )

    dataflow_process = DataFlowPythonOperator(
        task_id='dataflow-process',
        py_file='/home/airflow/gcs/dags/31_dataflow.py',
        options={
            'input': 'gs://{{ get_composer_gcs_bucket() }}/data/31_scenario/{{ execution_date }}/order.json',
            'output': 'gs://{{ get_composer_gcs_bucket() }}/data/31_scenario/{{ execution_date }}/order.csv'
        },
        dataflow_default_options={
            'project': 'sfeir-innovation',
            "staging_location": "gs://{{ get_composer_gcs_bucket() }}/data/dataflow",
            "temp_location": "gs://{{ get_composer_gcs_bucket() }}/data/dataflow"
        },
        dag=dag)

    import_to_sql = PythonOperator(
        task_id='import_to_sql',
        python_callable=sql_import,
        provide_context=True,
        templates_dict={
            'input_file': '/home/airflow/gcs/data/31_scenario/{{ execution_date }}/order.csv'
        },
        dag=dag)

    copy_file_to_gcs >> dataflow_process >> import_to_sql
