import os
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

## Configuration properties
ch1_ad_event_to_daily_backfill_config = Variable.get('ch1_2_ad_event_cleanup_config', deserialize_json=True)
support_emails = ch1_ad_event_to_daily_backfill_config['support_emails']

## DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 28).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'end_date': datetime(2019, 9, 10).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': support_emails,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='15 0 * * *', max_active_runs=1, catchup=True,
          params={
              'clickhouse_hostname1': ch1_ad_event_to_daily_backfill_config['clickhouse_hostname1'],
              'clickhouse_database1': ch1_ad_event_to_daily_backfill_config['clickhouse_database1']
          })

cleanup_data = DockerOperator(
    task_id='cleanup_data',
    command='./ad_event_cleanup.sh {{ params.clickhouse_hostname1 }} {{ params.clickhouse_database1 }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }}',
    image='airflow.ad.net:5000/ad.net/ch-cleanup:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)

cleanup_data
