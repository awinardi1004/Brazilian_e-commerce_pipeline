from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_snowflake_connection_from_airflow():
    """Create Snowflake connection dari Airflow Connection"""
    try:
        conn = BaseHook.get_connection('snowflake_conn')
        extra = conn.extra_dejson
        
        sf_conn = snowflake.connector.connect(
            user=conn.login,
            password=conn.password,
            account=extra.get('account', 'HT22908'),
            warehouse=extra.get('warehouse', 'ECOMMERCE_WH'),
            database=extra.get('database', 'ECOMMERCE_DB'),
            schema=conn.schema,
            role=extra.get('role', 'ECOMMERCE_ROLE')
        )
        logging.info("Snowflake connection established to {}".format(extra.get('account')))
        return sf_conn
    except Exception as e:
        logging.error("Snowflake connection failed: {}".format(e))
        raise

def clean_table_name(csv_filename):
    """
    Clean CSV filename untuk jadi table name
    Format: stg_nama_table (hapus olist_ dan _dataset)
    """
    # Remove extension and clean name
    base_name = csv_filename.replace('.csv', '')
    clean_name = base_name.replace('olist_', '').replace('_dataset', '')
    target_table = 'stg_{}'.format(clean_name)
    return target_table

def get_csv_file_path(csv_filename):
    """
    Get absolute file path untuk CSV file di Docker container
    """
    # Docker container path
    base_dir = '/usr/local/airflow/include/data source'
    
    # Full file path
    file_path = os.path.join(base_dir, csv_filename)
    
    logging.info("Looking for CSV file at: {}".format(file_path))
    
    # Debug: List files in directory
    if os.path.exists(base_dir):
        files = os.listdir(base_dir)
        logging.info("Files in data directory: {}".format(files))
    else:
        logging.warning("Data directory does not exist: {}".format(base_dir))
    
    return file_path

def read_csv_with_quotes(csv_file_path):
    """
    Read CSV file dengan handling quotes dan special characters
    """
    try:
        # Coba baca dengan berbagai parameter untuk handle quotes
        df = pd.read_csv(
            csv_file_path,
            encoding='utf-8',
            quotechar='"',
            escapechar='"',
            doublequote=True,
            na_values=['', 'NULL', 'null', 'NaN'],
            keep_default_na=False,
            low_memory=False
        )
        logging.info("Successfully read CSV: {} with {} rows, {} columns".format(
            os.path.basename(csv_file_path), len(df), len(df.columns)
        ))
        return df
    except pd.errors.ParserError as e:
        logging.warning("Parser error with standard method, trying alternative: {}".format(e))
        # Alternative method untuk file yang problematic
        try:
            df = pd.read_csv(
                csv_file_path,
                encoding='utf-8',
                on_bad_lines='skip',
                quoting=1,  # QUOTE_ALL
                quotechar='"',
                escapechar='\\',
                low_memory=False
            )
            logging.info("Alternative method successful for: {}".format(os.path.basename(csv_file_path)))
            return df
        except Exception as e2:
            logging.error("Failed to read CSV {}: {}".format(csv_file_path, e2))
            raise
    except UnicodeDecodeError:
        # Coba dengan encoding berbeda
        try:
            df = pd.read_csv(
                csv_file_path,
                encoding='latin-1',
                quotechar='"',
                escapechar='"',
                low_memory=False
            )
            logging.info("Successfully read with latin-1 encoding: {}".format(os.path.basename(csv_file_path)))
            return df
        except Exception as e:
            logging.error("Failed to read CSV with alternative encoding {}: {}".format(csv_file_path, e))
            raise
    except Exception as e:
        logging.error("Failed to read CSV {}: {}".format(csv_file_path, e))
        raise

def load_csv_to_snowflake(csv_filename):
    """
    Load single CSV file ke Snowflake dengan table naming convention
    """
    try:
        # Get full file path
        csv_file_path = get_csv_file_path(csv_filename)
        
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError("CSV file not found: {}".format(csv_file_path))
        
        logging.info("Processing CSV file: {}".format(csv_filename))
        
        # 1. Extract - Read CSV dengan quote handling
        df = read_csv_with_quotes(csv_file_path)
        
        if df.empty:
            logging.warning("CSV file {} is empty".format(csv_filename))
            return
        
        # 2. Transform - Clean column names dan data
        df_cleaned = clean_dataframe(df)
        
        # 3. Generate target table name
        target_table = clean_table_name(csv_filename)
        
        # 4. Load ke Snowflake
        load_dataframe_to_snowflake(df_cleaned, target_table)
        
        logging.info("Successfully loaded {} -> {} with {} rows".format(
            csv_filename, target_table, len(df_cleaned)
        ))
        
    except Exception as e:
        logging.error("Failed to load CSV {}: {}".format(csv_filename, e))
        raise

def clean_dataframe(df):
    """
    Clean dataframe - handle NaN values dan clean column names
    """
    # Clean column names (remove spaces, special chars)
    df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    
    # Replace NaN dengan None untuk compatibility Snowflake
    df = df.where(pd.notnull(df), None)
    
    # Convert columns dengan mixed types ke string untuk avoid errors
    for col in df.columns:
        if df[col].dtype == 'object':
            # Handle potential mixed types
            df[col] = df[col].astype(str)
            # Replace 'nan' string dengan None
            df[col] = df[col].replace('nan', None)
    
    return df

def load_dataframe_to_snowflake(df, table_name):
    """
    Load pandas dataframe ke Snowflake table
    """
    try:
        sf_conn = get_snowflake_connection_from_airflow()
        cursor = sf_conn.cursor()
        
        # Create table based on dataframe schema
        create_table_sql = generate_create_table_sql(df, table_name)
        cursor.execute(create_table_sql)
        logging.info("Created table: {}".format(table_name))
        
        # Insert data
        if len(df) > 0:
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns = ', '.join(['"{}"'.format(col) for col in df.columns])
            
            insert_sql = """
            INSERT INTO {} ({})
            VALUES ({})
            """.format(table_name, columns, placeholders)
            
            # Convert dataframe to list of tuples untuk executemany
            data_tuples = [tuple(x for x in row) for row in df.to_numpy()]
            
            # Batch insert untuk performance
            batch_size = 10000
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                logging.info("Inserted batch {}-{} into {}".format(
                    i + 1, min(i + batch_size, len(data_tuples)), table_name
                ))
        
        # Verify
        cursor.execute("SELECT COUNT(*) FROM {}".format(table_name))
        count = cursor.fetchone()[0]
        logging.info("Verified {} rows in table {}".format(count, table_name))
        
        cursor.close()
        sf_conn.close()
        
    except Exception as e:
        logging.error("Failed to load dataframe to {}: {}".format(table_name, e))
        raise

def generate_create_table_sql(df, table_name):
    """
    Generate CREATE TABLE SQL based on dataframe schema
    """
    columns = []
    for col_name, dtype in df.dtypes.items():
        # Map pandas dtypes ke Snowflake types
        if dtype in ['int64', 'int32']:
            snowflake_type = 'NUMBER'
        elif dtype in ['float64', 'float32']:
            snowflake_type = 'FLOAT'
        elif dtype == 'bool':
            snowflake_type = 'BOOLEAN'
        elif 'datetime' in str(dtype):
            snowflake_type = 'TIMESTAMP_NTZ'
        else:
            snowflake_type = 'STRING'  # Default untuk string dan unknown types
        
        columns.append('"{}" {}'.format(col_name, snowflake_type))
    
    create_sql = "CREATE OR REPLACE TABLE {} (\n    {}\n)".format(
        table_name, ',\n    '.join(columns)
    )
    
    return create_sql

# DAG Definition - SESUAIKAN DENGAN FILE YANG ADA
with DAG(
    'csv_to_snowflake_brazilian_ecommerce',
    default_args=default_args,
    description='Load Brazilian Ecommerce CSV files to Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['brazil-ecommerce', 'csv', 'snowflake']
) as dag:

    # SESUAIKAN DENGAN FILE YANG ACTUALLY EXIST
    csv_files = [
        'olist_geolocation_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv',
        'olist_sellers_dataset.csv',
        'product_category_name_translation.csv'
    ]
    # HAPUS file yang tidak ada: customers, orders, order_items, products
    
    start_task = PythonOperator(
        task_id='start_pipeline',
        python_callable=lambda: logging.info("Starting Brazilian E-commerce CSV to Snowflake Pipeline")
    )
    
    end_task = PythonOperator(
        task_id='end_pipeline', 
        python_callable=lambda: logging.info("Brazilian E-commerce CSV to Snowflake Pipeline Completed")
    )
    
    # Create tasks HANYA untuk file yang ada
    load_tasks = []
    for csv_file in csv_files:
        clean_name = clean_table_name(csv_file).replace('stg_', '')
        task = PythonOperator(
            task_id='load_{}'.format(clean_name),
            python_callable=load_csv_to_snowflake,
            op_args=[csv_file]
        )
        load_tasks.append(task)
    
    # Set dependencies - run semua tasks parallel
    start_task >> load_tasks >> end_task