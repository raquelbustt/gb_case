from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from scripts.first_load_tables import first_load_tables

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('first_load_tables', default_args=default_args, schedule_interval=None)

start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo "Starting first_load_tables DAG"',
    dag=dag
)

first_load_tables = PythonOperator(
    task_id='first_load_tables',
    python_callable=first_load_tables,
    dag=dag
)

finish_dag = BashOperator(
    task_id='finish_dag',
    bash_command='echo "first_load_tables DAG completed"',
    dag=dag
)

start_dag >> first_load_tables >> finish_dag
