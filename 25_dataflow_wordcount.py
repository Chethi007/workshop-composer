import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator

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

    dataflow_wordcount = DataflowTemplateOperator(
        task_id='dataflow_wordcount',
        template='gs://{{ get_composer_gcs_bucket() }}/data/dataflow/WordCount',
        parameters={
            'output': 'gs://{{ get_composer_gcs_bucket() }}/data/25_dataflow_wordcount/{{ execution_date }}/output'
        },
        dataflow_default_options={
            'project': 'sfeir-innovation'
        },
        dag=dag)
