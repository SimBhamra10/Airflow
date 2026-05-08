from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.events import EventsTimetable


special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=5, day=1, tz="Asia/Kolkata"),
        datetime(year=2026, month=5, day=3, tz="Asia/Kolkata"),
        datetime(year=2026, month=5, day=4, tz="Asia/Kolkata") 
    ]
)

@dag(
    dag_id="special_dates_dag",
    schedule=special_dates,
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=5, day=31, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True
)

def special_dates_dag():
    
    @task.python
    def run_task(**kwargs):
        event_Date = kwargs['logical_date']

        print("Extracting the data in {}".format(event_Date))


    first = run_task()



    [first]

special_dates_dag()
