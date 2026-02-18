from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.datasets import Dataset
from datetime import datetime

# Define a dataset
demo_dataset = Dataset("file://tmp/demo_dataset.txt")

@dag(
    dag_id="variables_and_dataset_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "variables", "datasets"],
)
def variables_dataset_demo():

    @task(outlets=[demo_dataset])
    def produce_data():
        # Read Airflow Variable
        name = Variable.get("greeting_name", default_var="User")
        
        message = f"Hello {name}!"
        
        # Write file
        with open("/tmp/demo_dataset.txt", "w") as f:
            f.write(message)

        print("File created with message:", message)

    produce_data()


variables_dataset_demo()
