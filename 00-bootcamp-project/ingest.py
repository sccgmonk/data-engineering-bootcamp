import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "my-project-de-434208"
location = "asia-southeast1"
bucket_name = "deb4-bootcamp-19"
# data = "addresses"
data=["addresses","events","order_items","orders","products","promos","user"]
# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "deb4-upload-file-to-gcs.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

# file_path = f"{DATA_FOLDER}/{data}.csv"
# destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"
# blob = bucket.blob(destination_blob_name)
# blob.upload_from_filename(file_path)
# # YOUR CODE HERE TO LOAD DATA TO GCS
for n in data:
    file_path = f"{DATA_FOLDER}/{n}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{n}/{n}.csv"

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)