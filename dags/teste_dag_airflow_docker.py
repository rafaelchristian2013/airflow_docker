from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
import pandas as pd

def get_data(start,end):
    with open('/opt/airflow/datalake/bronze/readme.txt', 'w') as f:
        f.write('readme')
    with open('/opt/airflow/datalake/silver/readme.txt', 'w') as f:
        f.write('readme')

with DAG('testes_dag_airflow_docker', start_date = datetime(2022,1,23),
        schedule_interval='@daily', max_active_runs=1, catchup=True) as dag:

        get_data = PythonOperator(
            task_id = 'get_data',
            python_callable = get_data,
            op_kwargs = {
                'start': '{{ data_interval_start | ds }}',
                'end': '{{ data_interval_end | ds }}'
                }
        )
        get_data