from airflow.sdk import dag, task
from more_itertools import first
from pendulum import datetime


@dag(
    dag_id="scheduler_preset_dag",
    start_date=datetime(year=2026, month=5, day=1, tz="Asia/Kolkata"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=False

)
def scheduler_preset_dag():
    
    @task.python
    def extract_task(**kwargs):
        print("Extracting data ....")
        ti = kwargs['ti']
        load_data = {'hdfs_data': [1,2,3,4,5],
                     'api_extracted_data': [123,234,435],
                     'db_data': [6,7,8,9],
                     'static_data': ['a','b','c','d'],
                     'weekend_flag': False}
        ti.xcom_push(key='return_result', value= load_data)

    @task.python
    def api_extracted_data(**kwargs):
        ti = kwargs['ti']
        print("Extracting API data")
        api_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_result')['api_extracted_data']
        transformed_api_data = [i*10 for i in api_data]
        ti.xcom_push(key = 'returned_result', value = transformed_api_data)

    
    @task.python
    def db_extracted_data(**kwargs):
        ti = kwargs['ti']
        print("Extracting DB data")
        db_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_result')['db_data']
        db_extracted_data = [i*100 for i in db_data]
        ti.xcom_push(key = 'returned_result', value = db_extracted_data)


    
    @task.python
    def hdfs_extracted_data(**kwargs):
        ti = kwargs['ti']
        print("Extracting HDFS data")
        hdfs_data = ti.xcom_pull(task_ids = 'extract_task', key = 'return_result')['hdfs_data']
        hdfs_extracted_data = [i*1000 for i in hdfs_data]
        ti.xcom_push(key = 'returned_result', value = hdfs_extracted_data)

    # Creating a decision branch based on the weekend flag
    @task.branch
    def decide_load(**kwargs):
        weekend_flag = kwargs['ti'].xcom_pull(task_ids = 'extract_task', key='return_result')['weekend_flag']
        if weekend_flag:
            return 'no_load_task' ##return the task id of the no load task
        else:
            return 'collect_data' ##return the task id of the collect data task


    @task.bash
    def collect_data(**kwargs):
        api_data = kwargs['ti'].xcom_pull(task_ids='api_extracted_data', key='returned_result')
        db_data = kwargs['ti'].xcom_pull(task_ids = 'db_extracted_data', key='returned_result')
        hdfs_data = kwargs['ti'].xcom_pull(task_ids = 'hdfs_extracted_data', key='returned_result')

        return f"echo 'Loaded Data: {api_data}, {db_data}, {hdfs_data}'"
    
    @task.bash
    def no_load_task(**kwargs):
        weekend_flag = kwargs['ti'].xcom_pull(task_ids = 'extract_task', key='return_result')['weekend_flag']
        if weekend_flag:
            return "echo 'No load on weekends'"
        else:
            return "echo 'Loading data...'"

    extract = extract_task()
    db_task = db_extracted_data()
    hdfs_task = hdfs_extracted_data()
    api_data = api_extracted_data()
    sink = collect_data()
    decision = decide_load()
    no_load = no_load_task()


    extract >> [api_data, hdfs_task, db_task] >> decision
    decision >> [sink, no_load]

scheduler_preset_dag()
