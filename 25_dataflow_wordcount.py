import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

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


with DAG(dag_id='25_dataflow_wordcount',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:
    
    dataflow_wordcount = DataFlowPythonOperator(
        task_id='dataflow-wordcount',
        py_file='/home/airflow/gcs/dags/dataflow_wordcount.py',
        options={
            'output': 'gs://{{ get_composer_gcs_bucket() }}/data/25_dataflow_wordcount/{{ execution_date }}/output'
        },
        dataflow_default_options={
            'project': 'YOUR-PROJECT-HERE',
            "staging_location": "gs://{{ get_composer_gcs_bucket() }}/data/dataflow",
            "temp_location": "gs://{{ get_composer_gcs_bucket() }}/data/dataflow"
        },
        dag=dag)
