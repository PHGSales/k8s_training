import logging
from time import sleep
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

dag = DAG(dag_id='dag_to_test_kubernetes_executor_5',
          start_date=datetime(2021, 1, 1),
          schedule_interval="@daily",
          catchup=False)


def func():
    a = 0
    while a < 30:
        sleep(10)
        logging.info(f"{a}")
        a += 1


task = PythonOperator(
    dag=dag,
    task_id='downloading_files_from_gcs',
    python_callable=func
)
