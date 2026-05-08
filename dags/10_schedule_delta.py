from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.interval import DeltaDataIntervalTimetable


@dag(
    dag_id="first_dag",
    schedule=DeltaDataIntervalTimetable(duration(days=3)),
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=5, day=31, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True
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
