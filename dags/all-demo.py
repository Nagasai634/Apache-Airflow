from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import os

FILE_PATH = "/opt/airflow/dags/data/demo.csv"
BUCKET_NAME = "apache-airflow-demo"   #  change this
DESTINATION_OBJECT = "uploads/eod.csv"


@dag(
    dag_id="advanced_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "gcp", "sensor", "branch"]
)
def advanced_etl_pipeline():

    # 1️ Wait for file
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=120,
    )

    # 2️ Branch logic
    def check_file():
        print("Checking file:", FILE_PATH)

        if not os.path.exists(FILE_PATH):
            print("File not found")
            return "skip_upload"

        size = os.path.getsize(FILE_PATH)
        print("File size:", size)

        if size > 0:
            return "upload_to_gcs"
        return "skip_upload"

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=check_file
    )

    # 3️ Upload to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=FILE_PATH,
        dst=DESTINATION_OBJECT,
        bucket=BUCKET_NAME,
        gcp_conn_id="gcp",
    )

    skip_upload = EmptyOperator(
        task_id="skip_upload"
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    wait_for_file >> branching
    branching >> upload_to_gcs >> end
    branching >> skip_upload >> end


advanced_etl_pipeline()