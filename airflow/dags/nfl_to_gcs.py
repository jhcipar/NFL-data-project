from datetime import datetime

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator


from google.cloud import storage

import nflfastpy as nfl
import pyarrow.parquet as pq
import pyarrow as pa

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'nfl_project')

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def download_schedule_data(**context):
    season_year = context['data_interval_start'].year
    schedule_data = pa.Table.from_pandas(nfl.load_schedule_data(year=season_year))
    dest_file_template = f"{AIRFLOW_HOME}/schedule_data_{season_year}.parquet"
    pq.write_table(schedule_data,dest_file_template)
    context["ti"].xcom_push(key='local_parquet_path',value=dest_file_template)

def download_pbp_data(**context):
    season_year = context['data_interval_start'].year
    pbp_data = pa.Table.from_pandas(nfl.load_pbp_data(year=season_year))
    dest_file_template = f"{AIRFLOW_HOME}/pbp_data_{season_year}.parquet"
    pq.write_table(pbp_data,dest_file_template)
    context["ti"].xcom_push(key='local_parquet_path',value=dest_file_template)

def download_injury_data(**context):
    season_year = context['data_interval_start'].year
    injury_data = pa.Table.from_pandas(nfl.load_injury_data(year=season_year))
    dest_file_template = f"{AIRFLOW_HOME}/injury_data_{season_year}.parquet"
    pq.write_table(injury_data,dest_file_template)
    context["ti"].xcom_push(key='local_parquet_path',value=dest_file_template)

def download_roster_data(**context):
    season_year = context['data_interval_start'].year
    roster_data = pa.Table.from_pandas(nfl.load_roster_data(year=season_year))
    dest_file_template = f"{AIRFLOW_HOME}/roster_data_{season_year}.parquet"
    pq.write_table(roster_data,dest_file_template)
    context["ti"].xcom_push(key='local_parquet_path',value=dest_file_template)

def upload_to_gcs(bucket, object_name, **context):
    local_file_template = context["ti"].xcom_pull(key='local_parquet_path')
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_template)


### written as a function

def download_parquetize_upload_dag(
    dag,
    callable_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:

        download_data_task = PythonOperator(
        task_id='download_data_task',
        python_callable=callable_template,
        provide_context=True,
        do_xcom_push=True,
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command='rm "{{ ti.xcom_pull(task_ids="download_data_task",key="local_parquet_path") }}"'
        )
        
        download_data_task >> local_to_gcs_task >> rm_task

#schedule_data

schedule_data_gcs_path_template="raw/schedule_data/{{ execution_date.strftime(\'%Y\') }}/schedule_data{{ execution_date.strftime(\'%Y\') }}.parquet"
schedule_data_parquet_file_template = AIRFLOW_HOME + '/local_parquet_data{{ execution_date.strftime(\'%Y\') }}.parquet'

schedule_data_dag = DAG(
    dag_id="schedule_data",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=schedule_data_dag,
    callable_template=download_schedule_data,
    local_parquet_path_template=schedule_data_parquet_file_template,
    gcs_path_template=schedule_data_gcs_path_template
)

#play-by-play data

pbp_data_gcs_path_template="raw/pbp_data/{{ execution_date.strftime(\'%Y\') }}/pbp_data{{ execution_date.strftime(\'%Y\') }}.parquet"
pbp_data_parquet_file_template = AIRFLOW_HOME + '/local_parquet_data{{ execution_date.strftime(\'%Y\') }}.parquet'

pbp_data_dag = DAG(
    dag_id="pbp_data",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=pbp_data_dag,
    callable_template=download_pbp_data,
    local_parquet_path_template=pbp_data_parquet_file_template,
    gcs_path_template=pbp_data_gcs_path_template
)

#injury data

injury_data_gcs_path_template="raw/injury_data/{{ execution_date.strftime(\'%Y\') }}/injury_data{{ execution_date.strftime(\'%Y\') }}.parquet"
injury_data_parquet_file_template = AIRFLOW_HOME + '/local_parquet_data{{ execution_date.strftime(\'%Y\') }}.parquet'

injury_data_dag = DAG(
    dag_id="injury_data",
    schedule_interval="@yearly",
    start_date=datetime(2009, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=injury_data_dag,
    callable_template=download_injury_data,
    local_parquet_path_template=injury_data_parquet_file_template,
    gcs_path_template=injury_data_gcs_path_template
)

#roster data

roster_data_gcs_path_template="raw/roster_data/{{ execution_date.strftime(\'%Y\') }}/roster_data{{ execution_date.strftime(\'%Y\') }}.parquet"
roster_data_parquet_file_template = AIRFLOW_HOME + '/local_parquet_data{{ execution_date.strftime(\'%Y\') }}.parquet'

roster_data_dag = DAG(
    dag_id="roster_data",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag=roster_data_dag,
    callable_template=download_roster_data,
    local_parquet_path_template=roster_data_parquet_file_template,
    gcs_path_template=roster_data_gcs_path_template
)

