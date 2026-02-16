from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime

with DAG(
    dag_id="upload_csv_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags = ['upload_to_gcs'],
) as dag:

    upload = LocalFilesystemToGCSOperator(
        task_id="upload_csv",
        src="/tmp/demo.csv",            # local file
        dst="demo.csv",                 # file name in bucket
        bucket="apache-airflow-demo",
        gcp_conn_id="gcp_connection",
    )
