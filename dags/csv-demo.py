from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

FILE_PATH = "/tmp/demo.csv"


# Task 1: Create CSV
def create_csv():
    data = {
        "name": ["Airflow", "Docker", "Kubernetes"]
    }
    df = pd.DataFrame(data)
    df.to_csv(FILE_PATH, index=False)
    print("CSV created")


# Task 2: Read CSV
def read_csv():
    df = pd.read_csv(FILE_PATH)
    print(df)


with DAG(
    dag_id="simple_csv_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,  
    catchup=False,
    tags=['simple_csv_demo'],
) as dag:

    create = PythonOperator(
        task_id="create_csv",
        python_callable=create_csv,
    )

    read = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv,
    )

    create >> read
