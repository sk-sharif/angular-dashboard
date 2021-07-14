import os
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

## Configuration properties
ch2_raw_user_events_backfill_from_impala = Variable.get('ch2_raw_user_events_backfill_from_impala', deserialize_json=True)
support_emails = ch2_raw_user_events_backfill_from_impala['support_emails']

## DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 1).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'end_date': datetime(2020, 10, 31).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': support_emails,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='15 0 * * *', max_active_runs=1, catchup=True,
          params={
              'clickhouse_hostname1': ch2_raw_user_events_backfill_from_impala['clickhouse_hostname1'],
              'clickhouse_database1': ch2_raw_user_events_backfill_from_impala['clickhouse_database1']
          })

backfill_data = DockerOperator(
    task_id='backfill_data',
    command='./raw_user_event_backfill_from_impala.sh {{ params.clickhouse_hostname1 }} {{ params.clickhouse_database1 }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }}',
    image='airflow.ad.net:5000/ad.net/ch-backfill:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)

backfill_data
