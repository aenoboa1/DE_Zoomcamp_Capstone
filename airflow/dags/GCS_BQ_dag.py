

from datetime import datetime
import os
import logging
from time import strftime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from utils import upload_to_gcs
from extract_data import fetch_data
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ENVIROMENT VARIABLES
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BQ_DATASET = os.environ.get("GCP_DATASET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")




#
file_name = '/{{execution_date.strftime(\'%Y\')}}'+'/chicago_crime_data_' + '{{execution_date.strftime(\'%Y\')}}'  + '-' + '{{execution_date.strftime(\'%m\')}}' + '.parquet' 
GCP_PATH_TEMPLATE = f"raw/{BQ_DATASET}/{{execution_date.strftime('%Y')}}/"+file_name

full_path = AIRFLOW_HOME + file_name


#PATHS
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'crimes_data_all')


INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"


# ARGS 
default_args = {
    "owner":"airflow",
    "depends_on_past": True,
    "retries":1
}

# DAG Declaration
with DAG(
    dag_id="gcs_bq_dag",
     default_args=default_args,
    

    start_date =datetime(2012,1,1), 
    schedule_interval = "0 1 3 * *",
    #start_date= datetime(2001,1,1), 
    #schedule_interval = "@daily",
    #catchup=False,
    max_active_runs=6,
    tags=['crime-de'],
) as dag:
    download_crime_data_task = PythonOperator(
        task_id = "extract_task",
        python_callable = fetch_data,
        op_kwargs = 
           {
            "month" : "{{execution_date.strftime('%m')}}" ,
            "year" : "{{execution_date.strftime('%Y')}}"
            }
        )
    data_to_gcs_task = PythonOperator(
        task_id = "data_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs={
            "bucket":BUCKET,
            "object_name" : GCP_PATH_TEMPLATE,
            "local_file" : f"{full_path}",

        }
    )

    remove_dataset_task = BashOperator(
        task_id = "remove_dataset_task",
        bash_command = f"rm {full_path} "
    )
    download_crime_data_task >> data_to_gcs_task >> remove_dataset_task





