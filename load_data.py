import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
import time

# Change this to your bucket name
BUCKET_NAME = "dez-m4-bucket"
DATASET_NAME = "dez_m4_dataset"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "key.json"
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bigquery_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
VARIANTS = ["green", "yellow", "fhv"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

DOWNLOAD_DIR = "./downloads"
files_already_downloaded = os.listdir(DOWNLOAD_DIR)

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = storage_client.bucket(BUCKET_NAME)


def download_file(url):
    file_name = url.split("/")[-1]
    save_path = os.path.join(DOWNLOAD_DIR, file_name)

    if file_name in files_already_downloaded:
        print(f"{file_name} already downloaded, skipping")
        return save_path

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, save_path)
        print(f"Downloaded: {file_name}")
        return save_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(storage_client)


def upload_to_gcs(file_path, max_retries=3):
    """Uploads a file to GCS, skipping if it already exists."""
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if blob.exists(storage_client):
        print(f"{blob_name} already exists in GCS, skipping upload.")
        return

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


def create_bigquery_external_table(variant):
    """Creates an external table in BigQuery for a given variant."""
    table_id = f"{bigquery_client.project}.{DATASET_NAME}.{variant}_tripdata"
    gcs_uri = f"gs://{BUCKET_NAME}/{variant}_tripdata_*.csv.gz"  # Explicitly match .csv.gz files

    table = bigquery.Table(table_id)
    external_config = bigquery.ExternalConfig("CSV")
    external_config.source_uris = [gcs_uri]
    external_config.autodetect = True  # Infer schema automatically
    external_config.options.skip_leading_rows = 1  # Skip header row
    external_config.options.compression = "GZIP"  # Explicitly set GZIP compression

    table.external_data_configuration = external_config

    try:
        table = bigquery_client.create_table(table)  # Creates the table
        print(f"BigQuery external table created: {table_id}")
    except Exception as e:
        print(f"Failed to create BigQuery table {table_id}: {e}")


def get_table_row_count(table_name):
    """Returns the number of rows in a given BigQuery table."""
    query = f"SELECT COUNT(*) AS row_count FROM `{bigquery_client.project}.{DATASET_NAME}.{table_name}`"

    try:
        query_job = bigquery_client.query(query)
        result = query_job.result()
        row_count = list(result)[0]["row_count"]
        return row_count
    except Exception as e:
        print(f"Failed to get row count for {table_name}: {e}")
        return None


def verify_row_counts(expected_num_rows):
    """Compares the actual row count in BigQuery tables with expected values."""
    for table, expected_count in expected_num_rows.items():
        actual_count = get_table_row_count(table)

        if actual_count is None:
            print(f"Skipping verification for {table} due to query failure.")
            continue

        if actual_count == expected_count:
            print(f"✅ {table}: Row count matches expected ({actual_count}).")
        else:
            print(f"❌ {table}: Expected {expected_count}, but found {actual_count}!")


if __name__ == "__main__":
    urls = []

    for variant in VARIANTS:
        for year in YEARS:
            if year == "2020" and variant == "fhv":
                continue
            for month in MONTHS:
                url = BASE_URL + f"{variant}/{variant}_tripdata_{year}-{month}.csv.gz"
                urls.append(url)

    with ThreadPoolExecutor(max_workers=4) as executor:
        files_to_upload = list(executor.map(download_file, urls))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, files_to_upload))  # Remove None values

    print("All files uploaded. Creating external tables...")

    # Create external tables in BigQuery
    for variant in VARIANTS:
        create_bigquery_external_table(variant)

    expected_num_rows = {
        "yellow_tripdata": 109047518,
        "green_tripdata": 7778101,
        "fhv_tripdata": 43244696,
    }

    verify_row_counts(expected_num_rows)
