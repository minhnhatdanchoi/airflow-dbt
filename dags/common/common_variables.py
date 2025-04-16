from dotenv import load_dotenv
import os
from airflow.models import Variable

class CommonDWH:
    SCHEMA_BRONZE = Variable.get('SCHEMA_BRONZE')
    DWH_CONNECTION = Variable.get('DWH_CONNECTION')
    MINIO_S3_CONN = Variable.get('MINIO_S3_CONN')

class CommonMINIO:
    MINIO_BUCKET = Variable.get("MINIO_BUCKET")

class ConfigStartTime:
    YEAR = Variable.get('YEAR')
class DBT:
    DBT_GOLD_TABLE = Variable.get('dbt_gold_table', default_var=None)
    DBT_RUN_ALL = Variable.get('dbt_run_all', default_var="True").lower() == "true"
