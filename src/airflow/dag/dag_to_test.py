from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def helloWorld():
    print("Hello World")


dag = DAG(dag_id="hello_world_dag",
          start_date=datetime(2021, 1, 1),
          schedule_interval="* * * * *",
          catchup=False)

task = PythonOperator(dag=dag, task_id="hello_world", python_callable=helloWorld)