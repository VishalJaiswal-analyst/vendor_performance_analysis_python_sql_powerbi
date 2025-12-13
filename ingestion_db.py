import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=os.path.join("logs", "ingestion_db.log"),  # relative path, no permission issues
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Global SQLAlchemy engine for the same SQLite DB
engine = create_engine('sqlite:///inventory.db')


def ingest_db(df, table_name, db_engine=engine):
    """This function will ingest the dataframe into DataBase Table"""
    logging.info(f"Writing table '{table_name}' to database...")
    df.to_sql(table_name, con=db_engine, if_exists='replace', index=False)  # index is bool
    logging.info(f"Table '{table_name}' ingestion complete.")


def load_raw_data():
    """This function will load the CSVs as DataFrame and ingest into db"""
    start = time.time()

    data_folder = 'data'
    logging.info("-----------Starting Ingestion-------------")

    for file in os.listdir(data_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(data_folder, file)
            logging.info(f"Reading file: {file_path}")
            df = pd.read_csv(file_path)

            table_name = file[:-4]  # remove .csv
            logging.info(f'Ingesting {file} into table: {table_name}')
            ingest_db(df, table_name)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('--------------Ingestion Complete-------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')


if __name__ == '__main__':
    load_raw_data()
