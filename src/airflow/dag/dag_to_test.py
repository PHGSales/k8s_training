from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

dag = DAG(dag_id='dag_to_test_gcs_download_dags', start_date=datetime(2021, 1, 1), schedule_interval='0 0 0 0 0',
          catchup=False)

task = GCSToLocalFilesystemOperator(
    dag=dag,
    task_id='downloading_dag_files_from_gcs',
    object_name='dags/dag_to_test_2.py',
    bucket='datalake_stg_test_dataplatform.stone.com.br',
    filename='/opt/airflow/dags/dag_to_test_2.py',
    gcp_conn_id='google_cloud_default')

task2 = GCSToLocalFilesystemOperator(
    dag=dag,
    task_id='downloading_dag_files_from_gcs',
    object_name='dags/dag_to_test_2.py',
    bucket='datalake_stg_test_dataplatform.stone.com.br',
    filename='/opt/airflow/dags/dag_to_test_2.py',
    gcp_conn_id='google_cloud_default')

task3 = GCSToLocalFilesystemOperator(
    dag=dag,
    task_id='downloading_dag_files_from_gcs',
    object_name='dags/dag_to_test_2.py',
    bucket='datalake_stg_test_dataplatform.stone.com.br',
    filename='/opt/airflow/dags/dag_to_test_2.py',
    gcp_conn_id='google_cloud_default')
