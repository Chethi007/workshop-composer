from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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


class MyCustomOperator(BaseOperator):
    """
    A new custom operator that can perform a lot of wonderful things (but doesn't).
    """
    template_fields = ('my_templated_field',)  # Beware the final comma, mandatory to declare a tuple
    ui_color = '#36c0f7'  # The RGC color code of the operator in Airflow UI

    # The @apply_defaults is Airflow magic to apply the content of the 'default_args' to the operator properties.
    # Look at the constructor parameter defined in the base class (BaseOperator) and compare to the 'default_args' dict.
    @apply_defaults
    def __init__(self,
                 my_templated_field,
                 *args,
                 **kwargs):
        """
        :param my_templated_field: The value of the templated field.
        :type my_templated_field: string
        """
        super(MyCustomOperator, self).__init__(*args, **kwargs)
        self.my_templated_field = my_templated_field  # This will be replaced at execution time as this field is declared in the class property 'template_fields'.

    def execute(self, context):
        # List objects.
        self.log.info('MyCustomOperator executed with value for the templated field : %s', self.my_templated_field)

        # Do stuff...


with DAG(dag_id='09_custom_operators',
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    using_static_value = MyCustomOperator(
        task_id='using_static_value',
        my_templated_field='Plouf'
    )

    using_templated_value = MyCustomOperator(
        task_id='using_templated_value',
        my_templated_field='Dag-run-id=[{{ run_id }}]'
    )
