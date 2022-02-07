from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
import os
import boto3



def write_data():
    with open('/opt/airflow/datalake/bronze/readme.txt', 'w') as f:
        f.write('readme')

    with open('/opt/airflow/datalake/silver/readme.txt', 'w') as f:
        f.write('readme')

def upload_s3():
    with open('/opt/airflow/auxiliary_data/key_id.txt') as f:
        key_id = f.read()

    with open('/opt/airflow/auxiliary_data/secret_key.txt') as f:
        secret_key = f.read()

    s3 = boto3.client(
    's3',
    aws_access_key_id=key_id,
    aws_secret_access_key=secret_key,
    )

    data_bronze = open('/opt/airflow/datalake/bronze/readme.txt', 'rb')
    s3.put_object(Bucket='datalake-airflow-docker', Key='bronze/readme.txt', Body=data_bronze)

def download_s3():
    with open('/opt/airflow/auxiliary_data/key_id.txt') as f:
        key_id = f.read()

    with open('/opt/airflow/auxiliary_data/secret_key.txt') as f:
        secret_key = f.read()

    s3 = boto3.client(
                    's3',
                    aws_access_key_id=key_id,
                    aws_secret_access_key=secret_key,
    )

    s3.download_file('datalake-airflow-docker', 'bronze/readme.txt', 'bronze_readme.txt')


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