from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="first_incremental_dag",
    schedule=CronDataIntervalTimetable('0 5 10 * MON-FRI', timezone="Asia/Kolkata"),
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=5, day=31, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True
)

def first_incremental_dag():
    
    @task.python
    def first_task(**kwargs):
        start_date = kwargs['data_interval_start']
        end_date = kwargs['data_interval_end']
        print("Extracting the data from {} to {}".format(start_date, end_date))

    @task.bash
    def second_task():
        return  "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"



    first = first_task()
    second = second_task() 


    [first,second]

first_incremental_dag()
