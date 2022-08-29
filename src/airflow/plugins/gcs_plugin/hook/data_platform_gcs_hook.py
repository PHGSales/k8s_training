from datetime import datetime
from typing import List
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class DataPlatformGCSHook(GCSHook):
    """
    This method is based on :class: airflow.providers.google.cloud.hooks.gcs.GCSHook. Just a new
    method was added: get_json_object.

    param gcp_conn_id: the gcp conn id
    type gcp_conn_id: str
    """

    def __init__(self, gcp_conn_id):
        super().__init__(gcp_conn_id)

    def get_all_file_updated_after(self, bucket_name: str, date: datetime, file_type: str) -> List:
        """
        Get all files updated after an specific date.

        :param bucket_name: the name of the bucket to get the information
        :type bucket_name: str

        :param date: the date to compare with the update of the file
        :type date: datetime

        :param file_type: the delimiter of the file (eg. .py, .csv, .json)
        :type file_type: str

        :return: the files that has to be downloaded
        :rtype: list
        """
        bucket_files = self.list(bucket_name=bucket_name, delimiter=file_type)
        files_to_download = list()
        for file in bucket_files:
            if self.is_updated_after(bucket_name=bucket_name, object_name=file, ts=date):
                files_to_download.append(file)
        return files_to_download