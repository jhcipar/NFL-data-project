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


def transfer_to_bq_dag(
    dag,
    data_type,
):
    with dag:

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id="gcs_2_bq_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"external_{data_type}",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{data_type}/2009/*.parquet"],
                },
            },
        )
        gcs_2_bq_ext_task 

def transfer_to_bq_partition_dag(
    dag,
    data_type,
):
    with dag:

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id="gcs_2_bq_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"external_{data_type}",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{data_type}/2009/*.parquet"],
                },
            },
        )

        CREATE_PART_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{data_type}_partitioned \
                    PARTITION BY DATE(game_timestamp) AS \
                    SELECT *, PARSE_TIMESTAMP('%F', game_date) AS game_timestamp \
                    FROM {BIGQUERY_DATASET}.external_{data_type};"

        bq_ext_2_part_task = BigQueryInsertJobOperator(
            task_id="bq_ext_2_part_task",
            configuration={
                "query": {
                    "query": CREATE_PART_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        gcs_2_bq_ext_task >> bq_ext_2_part_task


pbp_data_bq_dag = DAG(
    dag_id="pbp_data_bq_dag",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de','nfl']
)

transfer_to_bq_partition_dag(
    dag=pbp_data_bq_dag,
    data_type='pbp_data',
)

injury_data_bq_dag = DAG(
    dag_id="injury_data_bq_dag",
    schedule_interval="@yearly",
    start_date=datetime(2009, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de','nfl']
)

transfer_to_bq_dag(
    dag=injury_data_bq_dag,
    data_type='injury_data',
)

schedule_data_bq_dag = DAG(
    dag_id="schedule_data_bq_dag",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de','nfl']
)

transfer_to_bq_dag(
    dag=schedule_data_bq_dag,
    data_type='schedule_data',
)

roster_data_bq_dag = DAG(
    dag_id="roster_data_bq_dag",
    schedule_interval="@yearly",
    start_date=datetime(1999, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de','nfl']
)

transfer_to_bq_dag(
    dag=roster_data_bq_dag,
    data_type='roster_data',
)



