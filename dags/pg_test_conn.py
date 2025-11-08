from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

def test_postgres_connection():
    try:
        hook = PostgresHook(postgres_conn_id='pg_conn')
        conn = hook.get_conn()
        print("PostgreSQL Connection successful!")
        
        result = hook.get_first("SELECT version();")
        print(f"PostgreSQL Version: {result[0]}")
        
        result2 = hook.get_first("SELECT 1;")
        print(f"est query result: {result2[0]}")
        
        return True
    except Exception as e:
        print(f"PostgreSQL Connection failed: {e}")
        return False

def test_postgres_queries():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        queries = [
            "SELECT current_database();",
            "SELECT current_user;",
            "SELECT inet_server_addr();"
        ]
        
        for query in queries:
            result = hook.get_first(query)
            print(f"🔍 {query}: {result[0]}")
            
        return True
    except Exception as e:
        print(f"Query execution failed: {e}")
        return False

with DAG(
    'test_postgres_connection',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'postgres']
) as dag:

    test_connection = PythonOperator(
        task_id='test_connection',
        python_callable=test_postgres_connection
    )

    test_queries = PythonOperator(
        task_id='test_queries',
        python_callable=test_postgres_queries
    )

    test_connection >> test_queries