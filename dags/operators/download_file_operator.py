import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.base import BaseHook
from minio import Minio
from tempfile import NamedTemporaryFile
from common.common_variables import CommonMINIO

class DownloadFileFromMinioOperator(BaseOperator):
    def __init__(self, minio_bucket, minio_file_path,minio_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.minio_bucket = minio_bucket
        self.minio_file_path = minio_file_path
        self.minio_conn_id = minio_conn_id

    def execute(self, context: Context):
        connection = BaseHook.get_connection(self.minio_conn_id)
        minio_client = self.create_minio_client(connection)
        local_file_path = self.download_file_from_minio(minio_client, self.minio_file_path)
        self.log.info(f"File downloaded to: {local_file_path}")
        context['ti'].xcom_push(key='local_file_path', value=local_file_path)

    def create_minio_client(self, connection):
        """Create a MinIO client using the Airflow connection."""
        extras = connection.extra_dejson
        endpoint_url = extras.get("endpoint_url")
        access_key = connection.login
        secret_key = connection.password
        return Minio(
            endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

    def download_file_from_minio(self, minio_client,object_name ):
        self.log.info(
            f"Downloading file from MinIO bucket '{self.minio_bucket}', object path: '{object_name}'")
        with NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            temp_file_path = temp_file.name
            try:
                minio_client.fget_object(self.minio_bucket, object_name, temp_file_path)
                self.log.info(f"Downloaded file from MinIO: {temp_file_path}")
            except Exception as e:
                self.log.error(f"Failed to download file from MinIO: {e}")
                raise
        return temp_file_path
