import os
from datetime import datetime, timedelta
import csv

from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
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
    columns = ["WORD", "TIMES"]
    mysql = MySqlHook(mysql_conn_id='workshop_sql_conn_id')
    mysql.run("TRUNCATE WORDCOUNT")
    with open(input_file) as file:
        reader = csv.reader(file, delimiter=' ')
        data = list(reader)
        mysql.insert_rows('WORDCOUNT', data, target_fields=columns)


with DAG(dag_id='30_scenario',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         user_defined_macros={
             'get_composer_gcs_bucket': get_composer_gcs_bucket,
         }) as dag:

    copy_file_to_gcs = FileToGoogleCloudStorageOperator(
        task_id='copy_file_to_gcs',
        bucket='{{ get_composer_gcs_bucket() }}',
        dst='data/30_scenario/{{ execution_date }}/30_scenario.py',
        src='/home/airflow/gcs/dags/30_scenario.py',
        mime_type='text/x-python-script'
    )

    dataflow_wordcount = DataflowTemplateOperator(
        task_id='dataflow_wordcount',
        template='gs://{{ get_composer_gcs_bucket() }}/data/dataflow/WordCount',
        parameters={
            'inputFile': 'gs://{{ get_composer_gcs_bucket() }}/data/30_scenario/{{ execution_date }}/30_scenario.py',
            'output': 'gs://{{ get_composer_gcs_bucket() }}/data/30_scenario/{{ execution_date }}/wordcount.csv'
        },
        dataflow_default_options={
            'project': 'sfeir-innovation'
        },
        dag=dag)

    import_to_sql = PythonOperator(
        task_id='import_to_sql',
        python_callable=sql_import,
        provide_context=True,
        templates_dict={
            'input_file': '/home/airflow/gcs/data/30_scenario/{{ execution_date }}/wordcount.csv'
        },
        dag=dag)

    import_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='import_to_bigquery',
        bucket='{{ get_composer_gcs_bucket() }}',
        source_objects=[
            'data/30_scenario/{{ execution_date }}/wordcount.csv'
        ],
        destination_project_dataset_table='workshop_composer.wordcount',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=' ',
        schema_fields=[
            {
                "mode": "REQUIRED",
                "name": "WORD",
                "type": "STRING"
            },
            {
                "mode": "REQUIRED",
                "name": "TIMES",
                "type": "STRING"
            }
        ],
        dag=dag
    )

    copy_file_to_gcs >> dataflow_wordcount
    dataflow_wordcount >> import_to_sql
    dataflow_wordcount >> import_to_bigquery
