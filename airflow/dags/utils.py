import os 
import logging
from airflow import DAG
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
