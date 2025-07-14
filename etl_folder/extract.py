import pandas as pd
import logging


logging.basicConfig(
    filename="log_file.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_csv(raw_data):
    try:
        logging.info('-------------------------------------------')
        logging.info('Extracting the Data :)\n')

        logging.info(f"Starting API Extraction from {raw_data}")
        df = pd.read_csv(raw_data)
        return df
    
    except Exception as e:
        logging.error(f"Extraction Failed : {e}")
    
    finally:
        logging.info('Extraction Succesfull')
        logging.info('------------------------------------------------------')