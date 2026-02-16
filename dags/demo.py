from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id='max_demo',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['demo'],
)
def max_demo():
    
    @task
    def task_1():
        print("Task 1 executed successfully!")
        return "success"
    
    @task
    def task_2():
        print("Task 2 executed successfully!")
        return "success"
    
    # Simple pipeline
    t1 = task_1()
    t2 = task_2()

    t1 >> t2


max_demo()