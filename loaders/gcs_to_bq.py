import os
from google.cloud import bigquery, storage
import tempfile

PROJECT_ID  = os.getenv("GCP_PROJECT_ID")
BQ_DATASET  = os.getenv("BQ_DATASET")
GCS_BUCKET  = os.getenv("GCS_BUCKET")
CREDS_PATH  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_PATH

bq_client  = bigquery.Client(project=PROJECT_ID)
gcs_client = storage.Client(project=PROJECT_ID)

GOLD_PREFIX = "gold/anomalies/"
BQ_TABLE    = f"{PROJECT_ID}.{BQ_DATASET}.anomalies"

def get_parquet_files():
    bucket = gcs_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=GOLD_PREFIX)
    return [f"gs://{GCS_BUCKET}/{b.name}" for b in blobs
            if b.name.endswith(".parquet")]

def load_to_bigquery(parquet_files):
    if not parquet_files:
        print("No parquet files found in Gold layer.")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    print(f"Loading {len(parquet_files)} files into {BQ_TABLE}...")
    load_job = bq_client.load_table_from_uri(
        parquet_files,
        BQ_TABLE,
        job_config=job_config
    )
    load_job.result()
    table = bq_client.get_table(BQ_TABLE)
    print(f"Done! {table.num_rows} rows in {BQ_TABLE}")

if __name__ == "__main__":
    files = get_parquet_files()
    print(f"Found {len(files)} parquet files in gs://{GCS_BUCKET}/{GOLD_PREFIX}")
    load_to_bigquery(files)