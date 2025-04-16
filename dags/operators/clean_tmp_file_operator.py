from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import os

class CleanTmpFileOperator(BaseOperator):
    def __init__(self,file_path, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path

    def execute(self, context: Context):
        file_name = self.file_path.split('/')[-1]
        download_task_id = f"download_file_{file_name}"
        file_path = context['ti'].xcom_pull(task_ids=download_task_id, key='local_file_path')
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            self.log.info(f"Temporary file '{file_path}' deleted.")
        else:
            self.log.info(f"No file found at '{file_path}' to delete.")