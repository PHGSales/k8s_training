import logging
import os
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.exceptions import AirflowException
from typing import TYPE_CHECKING, Optional
from plugins.gcs_plugin.hook.data_platform_gcs_hook import DataPlatformGCSHook
if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataPlatformGCSToLocalFileSystemIncrementalOperator(GCSToLocalFilesystemOperator):
    def __init__(self, bucket_name: str, last_success_execution_date: datetime, local_file_system_location: str,
                 file_type: str, gcp_conn_id: str, break_if_fail: Optional[bool] = None,
                 gcs_hook: Optional[DataPlatformGCSHook] = None, **kwargs):
        super().__init__(bucket=bucket_name, gcp_conn_id=gcp_conn_id, **kwargs)
        self.bucket_name = bucket_name
        self.last_success_execution_date = last_success_execution_date
        self.local_file_system_location = local_file_system_location
        self.file_type = file_type
        self.break_if_fail = break_if_fail or False
        self.gcp_conn_id = gcp_conn_id
        self.gcs_hook = gcs_hook or DataPlatformGCSHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, context: 'Context'):
        files_to_download = self.gcs_hook.get_all_file_updated_after(bucket_name=self.bucket_name,
                                                                     date=self.last_success_execution_date,
                                                                     file_type=self.file_type)
        print(f"Files to download: {files_to_download}")
        print(f"Current path: {os.getcwd()}")
        logging.info(f"Files to download: {files_to_download}")
        for file in files_to_download:
            file = file[5:]
            print(f"File to download correct: {file}")
            try:
                logging.info(f"Downloading the file: {file}")
                self.gcs_hook.download(bucket_name=self.bucket_name,
                                       object_name=file,
                                       filename=f"{self.local_file_system_location}/{file}")
            except Exception as e:
                if self.break_if_fail:
                    raise AirflowException(f"Fail to download the file: {file} with the exception: {e}")
                else:
                    logging.info(f"Fail to download file: {file} with the error message: {e}")
