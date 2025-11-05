from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

@dag(
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
    default_args={"owner": "etl"},
    tags=["project-etl"],
)
def external_etl_dag():
    run_etl = BashOperator(
        task_id='run_external_etl',
        bash_command='python /usr/local/airflow/include/etl_loader.py',
    )

external_etl_dag()
