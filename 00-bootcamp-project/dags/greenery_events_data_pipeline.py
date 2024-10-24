import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "my-project-de-434208"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "events"


# def _extract_data(**context):
#     ds = context["ds"]

def _extract_data(ds):
    
    url =f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "event_id",
                "session_id",
                "page_url",
                "created_at",
                "event_type",
                "user",
                "order",
                "product",
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["event_id"],
                    each["session_id"],
                    each["page_url"],
                    each["created_at"],
                    each["event_type"],
                    each["user"],
                    each["order"],
                    each["product"]
                ]
                writer.writerow(data)
        return "load_data_to_gcs"    
    else :return "notthing"        
            




def _load_data_to_gcs(ds):
    keyfile_gcs = f"{DAGS_FOLDER}/deb4-upload-file-to-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )
      # Load data from Local to GCS
    bucket_name = "deb4-bootcamp-19"
    storage_client = storage.Client(

        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    partition = ds.replace("-", "")
    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    # destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}-{ds}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)





 def _load_data_from_gcs_to_bigquery(ds):
    keyfile_gcs = f"{DAGS_FOLDER}/deb4-upload-file-to-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb4-bootcamp-19"
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_events_data_pipeline",
    default_args=default_args,
    
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        # op_kwargs={"ds": "{{ ds }}"},
        
    )
    # notthing =EmptyOperator(
    #     task_id="notthing"
    # )

    # Load data to GCS
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
        
    )
    
    # Submit a Spark app to transform data
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/spark/pyspark/transform_event.py",
        # application="/opt/spark/pyspark/tranform.py",
        conn_id="my_spark",
        env_vars={'EXECUTION_DATE': '{{ ds }}'},
        # application_args=["ds"] ,
        # ,f"gs://deb4-bootcamp-19/raw/{BUSINESS_DOMAIN}/{DATA}/{partition}/{DATA}-{partition}.csv"
    )

    # load_data_from_gcs_to_bigquery = PythonOperator(
    #     task_id="load_data_from_gcs_to_bigquery",
    #     python_callable=_load_data_from_gcs_to_bigquery,
    # )
    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data 
    # >> transform_data>> load_data_from_gcs_to_bigquery
    # extract_data >>notthing