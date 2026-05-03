from airflow.sdk import dag, task
from more_itertools import first


@dag(
    dag_id="first_dag",
    schedule=None,
)
def first_dag():
    
    @task.python
    def first_task():
        print("This is the first task in the first DAG")

    @task.python
    def second_task():
        print("This is the second task in the first DAG")

    @task.python
    def third_task():
        print("This is the third task in the first DAG")    

    first = first_task()
    second = second_task() 
    third = third_task()

    first >> second >> third

first_dag()
