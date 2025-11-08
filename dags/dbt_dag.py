import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 8),
    catchup=False,
    dag_id="dbt_dag",
)
def dbt_snowflake_dag():
    
    dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
    
    # Task untuk menjalankan dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{dbt_executable_path} run --project-dir /usr/local/airflow/dags/dbt/dbt_pipeline --profiles-dir /usr/local/airflow/dags/dbt/dbt_pipeline",
    )
    
    # Task untuk menjalankan dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{dbt_executable_path} test --project-dir /usr/local/airflow/dags/dbt/dbt_pipeline --profiles-dir /usr/local/airflow/dags/dbt/dbt_pipeline",
    )
    
    dbt_run >> dbt_test


dbt_snowflake_dag = dbt_snowflake_dag()