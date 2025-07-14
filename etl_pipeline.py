from etl_folder.extract import extract_csv
from etl_folder.transform import transform_data
from etl_folder.load import load
import logging
import os
import sys

def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    raw_data = os.path.join(BASE_DIR, 'data', 'raw_data.csv')
    
    data = extract_csv(raw_data)
    
    df = transform_data(data)
    
    if df is None:
        logging.error("ETL Pipeline stopped because transform_data returned None.")
        sys.exit(1)
    
    load(df)
    
    logging.info('ETL Pipeline Finished Successfully :>')

if __name__ == "__main__":
    main()

