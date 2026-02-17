from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="xcom_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # ✅ must be string
    catchup=False,
    tags=["xcom"],
)
def xcom_demo():

    @task
    def generate_number():
        number = 10
        print(f"Generated number: {number}")
        return number   # Automatically pushed to XCom

    @task
    def multiply_number(num):
        result = num * 5
        print(f"Result after multiplying: {result}")
        return result

    value = generate_number()
    multiply_number(value)


xcom_demo()
