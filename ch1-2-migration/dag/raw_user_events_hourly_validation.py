import os
from datetime import datetime, timedelta

import pytz
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

## Configuration properties
ch1_2_hourly_validate_config = Variable.get('raw_user_events_hourly_validate_config', deserialize_json=True)
support_emails = ch1_2_hourly_validate_config['support_emails']

## DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 4).replace(tzinfo=pytz.timezone('America/Los_Angeles')),
    'email': support_emails,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='15 * * * *', max_active_runs=1, catchup=True,
          params={
              'clickhouse_hostname': ch1_2_hourly_validate_config['clickhouse_hostname1'],
              'clickhouse_database': ch1_2_hourly_validate_config['clickhouse_database1']
          })

validate_hdfs_raw_vs_ch = DockerOperator(
    task_id='validate_hdfs_raw_vs_ch',
    command='./validate.sh {{ params.clickhouse_hostname }} {{ params.clickhouse_database }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_time_string()[0:2] }} 1',
    image='airflow.ad.net:5000/ad.net/raw-user-events-validation:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)

validate_hdfs_stream_vs_ch = DockerOperator(
    task_id='validate_hdfs_stream_vs_ch',
    command='./validate.sh {{ params.clickhouse_hostname }} {{ params.clickhouse_database }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_time_string()[0:2] }}',
    image='airflow.ad.net:5000/ad.net/raw-user-events-validation:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)

validate_hdfs_stream_vs_hdfs_raw = DockerOperator(
    task_id='validate_hdfs_stream_vs_hdfs_raw',
    command='./validate_hdfs.sh {{ execution_date.in_tz(\'America/Los_Angeles\').to_date_string() }} {{ execution_date.in_tz(\'America/Los_Angeles\').to_time_string()[0:2] }}',
    image='airflow.ad.net:5000/ad.net/raw-user-events-validation:latest',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa'],
    dag=dag)

validate_hdfs_raw_vs_ch
validate_hdfs_stream_vs_ch
validate_hdfs_stream_vs_hdfs_raw