from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

dag = DAG(dag_id="dag_to_test_gcs_download_dags_2",
          start_date=datetime(2021, 1, 1),
          schedule_interval="@daily",
          catchup=False)


def hello_world():
    print("Hello World! Deu bom.")


tast = PythonOperator(
    dag=dag,
    task_id="test_hello_world",
    python_callable=hello_world)
