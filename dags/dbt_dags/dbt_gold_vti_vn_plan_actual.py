from airflow.utils.dates import days_ago
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from common.common_variables import CommonDWH

DWH_CONNECTION = CommonDWH.DWH_CONNECTION
project_path = '/opt/airflow/dbt/projects/financial_transform'

default_args = {
    'start_date': days_ago(1),
    'retries': 3
}

profile_config = ProfileConfig(
    profile_name="financial_dwh",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=DWH_CONNECTION,
        profile_args={"schema": 'bronze', "threads": 8},
    ),
)

# Tạo DAG cho model gold_vti_vn_plan_actual
gold_vti_vn_plan_actual = DbtDag(
    dag_id='dbt_financial_gold_vti_vn_plan_actual',
    project_config=ProjectConfig(project_path),
    profile_config=profile_config,
    render_config=RenderConfig(
        select=["+gold_vti_vn_plan_actual"]  # Chỉ định model cần chạy
    ),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=8,
    tags=['dbt']
)
