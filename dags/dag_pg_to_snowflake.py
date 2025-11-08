from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import snowflake.connector
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_postgres_connection_from_airflow():
    """Create PostgreSQL connection dari Airflow Connection"""
    try:
        conn = BaseHook.get_connection('pg_conn')
        pg_conn = psycopg2.connect(
            host=conn.host,
            database='data_source',
            user=conn.login,
            password=conn.password,
            port=conn.port
        )
        logging.info("PostgreSQL connection established to {}:{}".format(conn.host, conn.port))
        return pg_conn
    except Exception as e:
        logging.error("PostgreSQL connection failed: {}".format(e))
        raise

def get_snowflake_connection_from_airflow():
    """Create Snowflake connection dari Airflow Connection"""
    try:
        conn = BaseHook.get_connection('snowflake_conn')
        extra = conn.extra_dejson
        
        # PASTIKAN menggunakan account locator yang benar
        account = extra.get('account', 'HT22908')  # Gunakan HT22908, bukan CKNDHBG-KG92794
        
        sf_conn = snowflake.connector.connect(
            user=conn.login,
            password=conn.password,
            account=account,
            warehouse=extra.get('warehouse', 'ECOMMERCE_WH'),
            database=extra.get('database', 'ECOMMERCE_DB'),
            schema=conn.schema,
            role=extra.get('role', 'ECOMMERCE_ROLE')
        )
        logging.info("Snowflake connection established to {}".format(account))
        return sf_conn
    except Exception as e:
        logging.error("Snowflake connection failed: {}".format(e))
        raise

def extract_postgres_table(table_name, batch_size=10000):
    """
    Extract data dari PostgreSQL table
    """
    try:
        pg_conn = get_postgres_connection_from_airflow()
        cursor = pg_conn.cursor()
        
        # Get total rows
        cursor.execute("SELECT COUNT(*) FROM {}".format(table_name))
        total_rows = cursor.fetchone()[0]
        logging.info("Extracting {} rows from {}".format(total_rows, table_name))
        
        # Get column names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{}' 
            ORDER BY ordinal_position
        """.format(table_name))
        columns = [row[0] for row in cursor.fetchall()]
        logging.info("Columns: {}".format(columns))
        
        # Extract all data
        cursor.execute("SELECT {} FROM {}".format(', '.join(columns), table_name))
        data = cursor.fetchall()
        
        cursor.close()
        pg_conn.close()
        
        return {
            'table_name': table_name,
            'columns': columns,
            'data': data,
            'total_rows': total_rows
        }
        
    except Exception as e:
        logging.error("Extraction failed for {}: {}".format(table_name, e))
        raise

def load_to_snowflake(extracted_data, target_table_name):
    """
    Load data ke Snowflake dengan target table name
    """
    try:
        source_table = extracted_data['table_name']
        columns = extracted_data['columns']
        data = extracted_data['data']
        
        if not data:
            logging.info("No data to load for {}".format(target_table_name))
            return
        
        sf_conn = get_snowflake_connection_from_airflow()
        cursor = sf_conn.cursor()
        
        # Create table in Snowflake dengan target table name
        create_table_sql = """
        CREATE OR REPLACE TABLE {} (
            {}
        )
        """.format(target_table_name, ', '.join([f'"{col}" STRING' for col in columns]))
        
        cursor.execute(create_table_sql)
        logging.info("Created table {} in Snowflake".format(target_table_name))
        
        # Insert data
        if len(data) > 0:
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = """
            INSERT INTO {} 
            ({})
            VALUES ({})
            """.format(target_table_name, ', '.join([f'"{col}"' for col in columns]), placeholders)
            
            cursor.executemany(insert_sql, data)
            logging.info("Inserted {} rows into {}".format(len(data), target_table_name))
        
        # Verify data loaded
        cursor.execute("SELECT COUNT(*) FROM {}".format(target_table_name))
        count = cursor.fetchone()[0]
        logging.info("Verified {} rows in Snowflake table {}".format(count, target_table_name))
        
        cursor.close()
        sf_conn.close()
        
    except Exception as e:
        logging.error("Load failed for {}: {}".format(target_table_name, e))
        raise

def elt_table(source_table, target_table):
    """
    Combined ETL function untuk satu table dengan source dan target
    """
    try:
        logging.info("Starting ELT for table: {} -> {}".format(source_table, target_table))
        
        # Extract dari PostgreSQL
        extracted_data = extract_postgres_table(source_table)
        logging.info("Extraction completed for {}: {} rows".format(source_table, extracted_data['total_rows']))
        
        # Load ke Snowflake dengan target table name
        load_to_snowflake(extracted_data, target_table)
        logging.info("ELT completed successfully for {} -> {}".format(source_table, target_table))
        
    except Exception as e:
        logging.error("ELT failed for {} -> {}: {}".format(source_table, target_table, e))
        raise

# DAG Definition
with DAG(
    'DAG_pg_to_Snowflake',
    default_args=default_args,
    description='ELT Pipeline Brazilian Ecommerce dari PostgreSQL ke Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['brazil-ecommerce', 'postgresql', 'snowflake']
) as dag:

    # Table yang sesuai dengan database Anda
    tables_to_migrate = [
        'olist_customers_dataset',
        'olist_orders_dataset', 
        'olist_order_items_dataset',
        'olist_products_dataset'
    ]
    
    start_task = PythonOperator(
        task_id='start_pipeline',
        python_callable=lambda: logging.info("Starting Brazilian Ecommerce ELT Pipeline")
    )
    
    end_task = PythonOperator(
        task_id='end_pipeline',
        python_callable=lambda: logging.info("Brazilian Ecommerce ELT Pipeline Completed")
    )
    
    # Create tasks untuk table yang ada
    elt_tasks = []
    for table in tables_to_migrate:
        # Clean table name untuk task_id dan target table name
        clean_name = table.replace('olist_', '').replace('_dataset', '')
        target_table_name = 'stg_{}'.format(clean_name)  # Menjadi stg_customers, stg_orders, dll
        
        task = PythonOperator(
            task_id='elt_{}'.format(clean_name),
            python_callable=elt_table,
            op_args=[table, target_table_name]  # Pass both source and target table names
        )
        elt_tasks.append(task)
    
    # Set dependencies
    start_task >> elt_tasks >> end_task