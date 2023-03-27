from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import scripts.tables as t

start_date = datetime.now() - timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('tables_gb_dag', default_args=default_args, schedule_interval="00 01 * * *")

tasks = [
    ('vendas_ano_mes', t.vendas_ano_mes),
    ('vendas_marca_linha', t.vendas_marca_linha),
    ('vendas_marca_ano_mes', t.vendas_marca_ano_mes),
    ('vendas_linha_ano_mes', t.vendas_linha_ano_mes),
]

start_dag = PythonOperator(
    task_id='start_dag',
    python_callable=lambda: print("Starting extraction_tables gb DAG"),
    dag=dag
)

finish_dag = PythonOperator(
    task_id='finish_dag',
    python_callable=lambda: print("extraction_tables gb DAG completed"),
    dag=dag
)

for task_id, function in tasks:
    task = PythonOperator(
        task_id=f"{task_id}_sql",
        python_callable=function,
        dag=dag
    )

    task_csv = PythonOperator(
        task_id=f"{task_id}_csv",
        python_callable=t.read_and_save_data_to_csv,
        op_kwargs={'table_name': task_id},
        dag=dag
    )

    start_dag >> task >> task_csv >> finish_dag
