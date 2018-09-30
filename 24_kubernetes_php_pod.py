from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator

yesterday = datetime.combine(datetime.today() - timedelta(1),datetime.min.time())

default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with models.DAG(
        dag_id='24_kubernetes_php_pod',
        schedule_interval=timedelta(days=1),
        catchup=False,
        default_args=default_args) as dag:

    kubernetes_php_pod = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='kubernetes_php_pod',
        name='pod-php',
        namespace='default',
        image='php',
        cmds=['php'],
        arguments=['-v'])
