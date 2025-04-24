from __future__ import annotations
import logging
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from operators.download_file_operator import DownloadFileFromMinioOperator
from operators.create_table_operator import CreateTableFromMetadataOperator
from operators.clean_load_data_to_bronze_fact_operator import CleanAndLoadDataFactOperator
from operators.clean_tmp_file_operator import CleanTmpFileOperator
from airflow.utils.dates import days_ago
from common.common_variables import CommonDWH, CommonMINIO, ConfigStartTime
from minio import Minio

log = logging.getLogger(__name__)
MINIO_S3_CONN = CommonDWH.MINIO_S3_CONN
DWH_CONNECTION = CommonDWH.DWH_CONNECTION
MINIO_BUCKET = CommonMINIO.MINIO_BUCKET
SCHEMA_BRONZE = CommonDWH.SCHEMA_BRONZE
YEAR = ConfigStartTime.YEAR
year = YEAR
MINIO_FILE_PATH = f"fact/{year}"
log.info(f"MINIO_FILE_PATH set to: {MINIO_FILE_PATH}")
MINIO_FILE_PATH = f"fact/{year}"

log.info(MINIO_FILE_PATH)
connection = BaseHook.get_connection(MINIO_S3_CONN)
extras = connection.extra_dejson
access_key = connection.login
secret_key = connection.password
MINIO_CLIENT = Minio(
    extras["endpoint_url"],
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)
log.info(f"Scanning for Excel files fact in MinIO bucket '{MINIO_BUCKET}' at '{MINIO_FILE_PATH}'")
excel_file = [
    obj.object_name
    for obj in MINIO_CLIENT.list_objects(MINIO_BUCKET, prefix=MINIO_FILE_PATH, recursive=True)
    if obj.object_name.endswith(".xlsx") or obj.object_name.endswith(".xls")
]
if not excel_file:
    log.warning(f"No Excel files found in folder '{MINIO_FILE_PATH}' on MinIO server.")
else:
    log.info(f"Found {len(excel_file)} Excel files: {excel_file}")

for file_path in excel_file:
    local_file_path = f"/tmp/{file_path.split('/')[-1]}"
    dag_id = f"ELT_FACT_{file_path.split('/')[-1].replace('.xlsx', '').replace('.xls', '')}"
    # dag_id = f"ELT_{file_path.replace('/', '_').replace('.xlsx', '')}"
# companies = ['VTI']
#
# for name_company in companies:
#     dag_id = f'Financial_ETL_{name_company}'
    default_args = {
        'start_date': days_ago(1),
        'retries': 3
    }
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Dynamic DAG for file: {file_path.split('/')[-1]}",
        schedule_interval='0 0 10 * *',
        max_active_runs=1,
        concurrency=1,
        catchup=True,
        tags=['ETL', 'FACT',str(year)],
    ) as dag:
        download_task = DownloadFileFromMinioOperator(
            task_id=f"download_file_{file_path.split('/')[-1]}",
            minio_bucket=MINIO_BUCKET,
            minio_file_path=file_path,
            minio_conn_id=MINIO_S3_CONN
        )

        create_table_task = CreateTableFromMetadataOperator(
            task_id='create_table',
            dwh_connection=DWH_CONNECTION,
            schema_bronze=SCHEMA_BRONZE,
            file_path=local_file_path
        )

        load_data_task = CleanAndLoadDataFactOperator(
            task_id="load_data",
            dwh_connection=DWH_CONNECTION,
            schema_bronze=SCHEMA_BRONZE,
            year = year,
            file_path=local_file_path
        )

        clean_tmp_file_task = CleanTmpFileOperator(
            task_id="clean_tmp_file",
            trigger_rule="all_done",
            file_path=local_file_path
        )

        download_task >> create_table_task >> load_data_task
        [download_task, create_table_task, load_data_task] >> clean_tmp_file_task

        globals()[dag_id] = dag

