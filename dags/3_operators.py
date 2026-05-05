from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="operators_dag",
    schedule=None,
)
def operators_dag():
    
    @task.python
    def first_task():
        print("This is the first task in the first DAG")

    @task.python
    def second_task():
        print("This is the second task in the first DAG")

    @task.bash
    def bash_task():
        bash_command="echo Hello from Airflow!"
        return bash_command

    bash_task2 = BashOperator(
                    task_id="print_hello",
                    bash_command="echo Hello from Airflow!"
                )

    first = first_task()
    second = second_task() 
    third = bash_task()
    version = bash_task2

    first >> second >> third >> version

operators_dag()
