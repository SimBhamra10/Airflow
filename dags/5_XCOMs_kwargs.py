from airflow.sdk import dag, task
from more_itertools import first


@dag(
    dag_id="xcoms_dag_kwargs",
    schedule=None,
)
def xcoms_dag_kwargs():
    
    @task.python
    def first_task(**kwargs):

        ti = kwargs['ti']
        print("Extracting data.. This is the first task")
        fetched_data  = {"data": [1,2,3,4]}
        ti.xcom_push(key = 'return_result', value = fetched_data)
        

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']

        fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')
        fetched_data = fetched_data['data']
        print("transforming data ... this is the second task")
        transformed_data = fetched_data*2
        transformed_data_dict = {"trans_data": transformed_data}
        ti.xcom_push(key = 'return_result', value = transformed_data_dict)
        

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids = 'second_task', key = 'return_result')['trans_data']
        return load_data

    first = first_task()
    second = second_task() 
    third = third_task()

    first >> second >> third

xcoms_dag_kwargs()
