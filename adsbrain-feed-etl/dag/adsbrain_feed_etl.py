from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import os

DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

# Configuration properties
adsbrain_feed_etl_config = Variable.get('adsbrain_feed_etl_config', deserialize_json=True)
support_emails = adsbrain_feed_etl_config['support_emails']
sftp_username = adsbrain_feed_etl_config['sftp']['username']
sftp_password = adsbrain_feed_etl_config['sftp']['password']


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 20).replace(tzinfo=pendulum.timezone('America/Los_Angeles')),
    'email': support_emails,
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval='0 10 * * *', max_active_runs=1, catchup=False)
data_dir = '/usr/local/airflow/data/' + DAG_NAME

# Operator definition
download_adsbrain_report = DockerOperator(
    task_id='download_adsbrain_report',
    command='./download_adsbrain_report.sh {{ execution_date.in_timezone("America/Los_Angeles").to_date_string() }} %s %s' % (sftp_username, sftp_password),
    image='airflow.ad.net:5000/ad.net/adsbrain-feed',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa', data_dir + ':/data'],
    retries=4,
    retry_delay=timedelta(minutes=60),
    dag=dag)

ingest_adsbrain_report = DockerOperator(
    task_id='ingest_adsbrain_report',
    command='./ingest_adsbrain_report.sh {{ execution_date.in_timezone("America/Los_Angeles").to_date_string() }}',
    image='airflow.ad.net:5000/ad.net/adsbrain-feed',
    volumes=['/root/.ssh/etl_rsa:/root/.ssh/id_rsa', data_dir + ':/data'],
    retries=1,
    retry_delay=timedelta(minutes=10),
    dag=dag)

download_adsbrain_report >> ingest_adsbrain_report
