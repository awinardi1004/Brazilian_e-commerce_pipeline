from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def test_snowflake_connection():
    """
    Test koneksi Snowflake sederhana
    """
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        print("nowflake connection successful!")
        
        # Test basic query
        result = hook.get_first("SELECT CURRENT_VERSION() as version")
        print(f"Snowflake Version: {result[0]}")
        
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        raise

with DAG(
    'simple_test_snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    test_conn = PythonOperator(
        task_id='test_snowflake_connection',
        python_callable=test_snowflake_connection
    )