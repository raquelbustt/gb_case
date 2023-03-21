from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.podcast import table_5, table_6, table_7
from scripts.conection import get_access_token

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('podcast_dag', default_args=default_args, schedule_interval="00 01 * * *")

client_id = '814fa36ad52c472a9d55d6dde043610f'
client_secret = '49b20ade9e534c1488b67aacb89b0ff2'

access_token = get_access_token(client_id, client_secret)

start_dag = PythonOperator(
    task_id='start_dag',
    python_callable=lambda: print("Starting extraction_podcast DAG"),
    dag=dag
)

finish_dag = PythonOperator(
    task_id='finish_dag',
    python_callable=lambda: print("extraction_podcast DAG completed"),
    dag=dag
)

table_5 = PythonOperator(
    task_id='table_5',
    python_callable=table_5,
    op_kwargs={'access_token': access_token},
    dag=dag
)

table_6 = PythonOperator(
    task_id='table_6',
    python_callable=table_6,
    op_kwargs={'access_token': access_token},
    dag=dag
)

table_7 = PythonOperator(
    task_id='table_7',
    python_callable=table_7,
    dag=dag
)

start_dag >> table_5 >> table_6 >> table_7 >> finish_dag