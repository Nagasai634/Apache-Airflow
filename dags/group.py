from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(
    dag_id="group_demo",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags = ['group'],
)
def group():

    @task
    def a():
        print("a")

    @task_group
    def my_group():

        @task
        def b():
            print("b")

        @task
        def c():
            print("c")

        b() >> c()

    a() >> my_group()

group()