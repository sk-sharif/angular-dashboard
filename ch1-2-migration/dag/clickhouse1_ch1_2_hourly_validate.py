import os
from datetime import datetime, timedelta

import airflow
import pendulum
import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.sensors import ExternalTaskSensor
import dateutil.parser
import airflow.macros

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

## Configuration properties
clickhouse1_ch1_2_hourly_validate = Variable.get('clickhouse1_ch1_2_hourly_validate', deserialize_json=True)
support_emails = clickhouse1_ch1_2_hourly_validate['support_emails']

## DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 16).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': support_emails,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='15 * * * *', max_active_runs=1, catchup=True,
          params={
              'clickhouse_hostname1': clickhouse1_ch1_2_hourly_validate['clickhouse_hostname1'],
              'clickhouse_database1': clickhouse1_ch1_2_hourly_validate['clickhouse_database1'],
              'clickhouse_hostname2': clickhouse1_ch1_2_hourly_validate['clickhouse_hostname2'],
              'clickhouse_database2': clickhouse1_ch1_2_hourly_validate['clickhouse_database2']
          })


validate_data = DockerOperator(
    task_id='validate_data',
    command='./hourly_validate.sh {{ params.clickhouse_hostname1 }} {{ params.clickhouse_database1 }} {{ params.clickhouse_hostname2 }} {{ params.clickhouse_database2 }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_time_string()[0:2] }}',
    image='airflow.ad.net:5000/ad.net/ch-hourly-validation:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)


validate_data
