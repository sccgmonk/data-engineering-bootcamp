import json

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "my-project-de-434208"
location = "asia-southeast1"
bucket_name = "deb4-bootcamp-19"
data = "addresses"


# Prepare and Load Credentials to Connect to GCP Services
keyfile_bigquery = "load_data_to_bigquery_su.json"
service_account_info_bigquery = json.load(open(keyfile_bigquery))
credentials_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_bigquery
)

# Load data from GCS to BigQuery
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials_bigquery,
    location=location,
)

source_data_in_gcs = f"gs://{bucket_name}/processed/{BUSINESS_DOMAIN}/{data}/*.parquet"
table_id = f"{project_id}.deb_bootcamp.{data}"
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.PARQUET,
    # autodetect=True,
    schema=[
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
    ],
)
job = bigquery_client.load_table_from_uri(
    source_data_in_gcs,
    table_id,
    job_config=job_config,
    location=location,
)
job.result()

table = bigquery_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


data2="orders"
dt = "2021-02-10"
partition = dt.replace("-", "")
source_data_in_gcs = f"gs://{bucket_name}/processed/{BUSINESS_DOMAIN}/{data2}/*.parquet"
table_id = f"{project_id}.deb_bootcamp.{data2}${partition}"
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.PARQUET,
    # autodetect=True,
    schema=[
        bigquery.SchemaField("order_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("order_cost", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("shipping_cost", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("order_total", bigquery.SqlTypeNames.FLOAT),
        bigquery.SchemaField("tracking_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("shipping_service", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("estimated_delivery_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("delivered_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("status", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("user", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("promo", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
        
       
        
    ],
)
job = bigquery_client.load_table_from_uri(
    source_data_in_gcs,
    table_id,
    job_config=job_config,
    location=location,
)
job.result()

table = bigquery_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
