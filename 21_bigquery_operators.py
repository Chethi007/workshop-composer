import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

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


with DAG(dag_id='21_bigquery_operators',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:

    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id='bq_to_gcs',
        source_project_dataset_table='bigquery-public-data:samples.gsod',
        destination_cloud_storage_uris=['gs://{{ get_composer_gcs_bucket() }}/data/21_bigquery_operators/{{ execution_date }}/gsod-*.csv']
    )
