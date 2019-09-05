import configparser
import psycopg2
import pyspark
import logging
import boto3
from io import StringIO
from typing import Any, List, Dict
from pyspark.sql.functions import *

# Import relevant utility functions
from utils.connections import get_redshift_connection
from utils.run_sql import execute_sql_file
from utils.check_tables_exist import check_redshift_table_exists

# Import relevant pipeline validation checks
from pipeline_validation_checks.count_rows import count_table_rows


def _create_analytics_tables(config, logger, cursor, sql_file_path: str = '/home/workspace/racing_pipeline/sql/create_analytics_tables.sql') -> None:
    """
    Executes SQL files containing queries that create tables in Redshift.
    """
    
    logger.warn('About to create analytics tables.')
    
    # Create staging tables
    execute_sql_file(sql_file_path, cursor, logger)
    
    logger.warn('Completed creating analytics tables.')
    

def _write_dataframe_to_s3(config, logger, df: pyspark.sql.DataFrame, df_name: str) -> None:
    """
    Converts a PySpark DataFrame to Pandas, before writing out to a CSV file stored in Amazon
    S3, in the given bucket pulled from the config object.
    
    """
    logger.warn(f'About to write dataframe: {df_name} as CSV to S3')
    
    # Convert Pyspark dataframe to Pandas
    pd_df = df.toPandas()
    
    # Get S3 details
    s3 = boto3.resource('s3',
                        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])
    
    #Write Pandas df to CSV stored locally
    csv_buff = StringIO()
    
    pd_df.to_csv(csv_buff, sep=',', index = False)
    
    # Write to S3
    s3.Object(config['S3']['BUCKET_NAME'], f'{df_name}.csv').put(Body=csv_buff.getvalue())
    
    logger.warn(f'Finished writing dataframe: {df_name} as CSV to S3')



def load(df_1: pyspark.sql.DataFrame, df_2: pyspark.sql.DataFrame, df_3: pyspark.sql.DataFrame, config, logger) -> None:
    """
    Orchestrates loading functions.
    """
    # Establish database connection
    connection = get_redshift_connection(config, logger)
    cur = connection.cursor()
    
    logger.warn('About to begin loading Analytics tables.')
    
    # Create Redshift Analytics tables
    _create_analytics_tables(config, logger, cur)
    
    # Check that tables were created correctly
    check_redshift_table_exists(['customers', 'races', 'race_horses'], 'horse_racing', cur)
    
    # Write transformed dataframes to S3
    _write_dataframe_to_s3(config, logger, df_1, df_name = 'customers')
    
    _write_dataframe_to_s3(config, logger, df_2, df_name = 'races')
    
    _write_dataframe_to_s3(config, logger, df_3, df_name = 'race_horses')
    
    # Copy transformed dataframe CSVs to Redshift Analytics tables
    execute_sql_file('/home/workspace/racing_pipeline/sql/copy_to_analytics_tables.sql', cur, logger)
    
    # Define expected row counts for Analytics tables
    expected = {'customers': 25185,
               'races': 39893,
               'race_horses': 44428}
    
    # Validate that the CSVs were written to the tables
    count_table_rows(expected, cur, logger)
    
    logger.warn('Finished loading Analytics tables.')