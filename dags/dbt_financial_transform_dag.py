from airflow.utils.dates import days_ago
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from common.common_variables import CommonDWH


DWH_CONNECTION = CommonDWH.DWH_CONNECTION
project_path = '/opt/airflow/dbt/projects/financial_transform'
# profile_file_path = '/opt/airflow/dbt/.dbt/profiles.yml'

default_args = {
    'start_date': days_ago(1),
    'retries': 3
}
profile_config = ProfileConfig(
    profile_name="financial_dwh",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=DWH_CONNECTION,
        profile_args={"schema": 'bronze',"threads": 8},
    ),
)
#learn more: https://www.astronomer.io/docs/learn/airflow-dbt/

dbt_financial_transform = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(project_path),
    profile_config=profile_config,
    # normal dag parameters
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=8,
    dag_id="dbt_financial_transform_dag",
    tags=["dbt"]
)