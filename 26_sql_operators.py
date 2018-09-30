import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator

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


with DAG(dag_id='26_sql_operators',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:

    sql_operators = MySqlToGoogleCloudStorageOperator(
        task_id='sql_operators',
        sql='SELECT * FROM PERSONS;',
        bucket='{{ get_composer_gcs_bucket() }}',
        filename='data/26_sql_operators/{{ execution_date }}/persons.json',
        mysql_conn_id='workshop_sql_conn_id',
        dag=dag)
