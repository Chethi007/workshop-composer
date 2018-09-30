import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

yesterday = datetime.combine(datetime.today() - timedelta(1),datetime.min.time())

default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def get_composer_gcs_bucket():
    return os.environ['GCS_BUCKET']


with DAG(dag_id='20_gcs_operators',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:

    copy_file = FileToGoogleCloudStorageOperator(
        task_id='copy_file',
        bucket='{{ get_composer_gcs_bucket() }}',
        dst='data/20_gcs_operators/{{ execution_date }}/20_gcs_operators.py',
        src='/home/airflow/gcs/dags/20_gcs_operators.py',
        mime_type='text/x-python-script'
    )
