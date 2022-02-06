from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
import os

def write_data():
    with open('/opt/airflow/datalake/bronze/readme.txt', 'w') as f:
        f.write('readme')

    with open('/opt/airflow/datalake/silver/readme.txt', 'w') as f:
        f.write('readme')

def remove_data():
    os.remove("/opt/airflow/datalake/bronze/readme.txt")

    os.remove('/opt/airflow/datalake/silver/readme.txt')

with DAG('testes_dag_airflow_docker', start_date = datetime(2022,1,23),
        schedule_interval='@daily', max_active_runs=1, catchup=True) as dag:

        write_data = PythonOperator(
            task_id = 'write_data',
            python_callable = write_data
        )

        remove_data = PythonOperator(
            task_id = 'remove_data',
            python_callable = remove_data
        )

        write_data >> remove_data