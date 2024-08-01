from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from crunchbase_legacy_code import sample_function, SampleClass


# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="crunchbase_dag_v2.0", 
    start_date=datetime(2024, 7, 31), 
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=['crunchbase'],
    ) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def python_task():
        print("python_task")
        sample_function()
        sample = SampleClass(name='Airflow')
        sample.print_name()

    # Set dependencies between tasks
    hello >> python_task()