from datetime import datetime, timedelta
from airflow import DAG
from gcs_plugin.operators.data_platform_gcs_to_local_file_system_incremental_operator import \
    DataPlatformGCSToLocalFileSystemIncrementalOperator

default_args = {
    "owner": "data_platform_active_ingestion",
    "start_date": datetime(2018, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}

dag = DAG(
    dag_id="test_download_from_gcs2",
    schedule_interval="@daily",
    default_args=default_args,
)

task = DataPlatformGCSToLocalFileSystemIncrementalOperator(
    dag=dag,
    task_id="task_1",
    bucket_name="datalake_stg_test_dataplatform.stone.com.br",
    last_success_execution_date=datetime.strptime("2022-8-9T18:11:00", '%Y-%m-%dT%H:%M:%S'),
    local_file_system_location="",
    gcp_conn_id="google_cloud_default",
    file_type=".py",
    break_if_fail=False)
