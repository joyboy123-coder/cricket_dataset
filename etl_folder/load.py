import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from dotenv import load_dotenv
import os


load_dotenv()

user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
table = os.getenv('SNOWFLAKE_TABLE')

if not table:
    raise ValueError("❗ Environment variable TABLE_NAME is missing or empty.")

def load(df):
    try:
        logging.info('\nData Loading\n')
        logging.info('Connecting to Snowflake')
    
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            insecure_mode=True
        )

        logging.info('Loading Clean Data into Snowflake')

        df = df.reset_index(drop=True)

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            schema=schema,
            database=database,
            auto_create_table=True
        )
        
        if success:
            logging.info(f'Uploaded {nrows} to Snowflake table: {table}')
        else:
            logging.error('Upload Failed')

    except Exception as e:
        logging.error(f'Failed to Load Data into Snowflake: {e}')
        raise
    
    finally:
         logging.info(' Data Loading Completed')
         logging.info('--------------------------------------------\n')

