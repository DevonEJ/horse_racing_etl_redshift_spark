import configparser
import psycopg2
import logging
import boto3
from typing import Any, List, Dict

# Import relevant utility functions
from utils.connections import get_redshift_connection
from utils.run_sql import execute_sql_file
from utils.truncate_redshift_tables import truncate_table
from utils.check_tables_exist import check_redshift_table_exists

# Import relevant pipeline validation checks
from pipeline_validation_checks.count_rows import count_table_rows
    

def _cleanup_staging_environment(config, logger) -> None:
    """
    Truncates staging tables read as a list from the configuration object passed in. Checks if tables
    exist in the schema or not before executing truncate statement.
    
    -- config - configparser config object.
    -- logger - logging object.
    """
    logger.warn('Starting cleanup of staging environment in Redshift.')
        
    # Establish database connection
    connection = get_redshift_connection(config, logger)
    cur = connection.cursor()
    
    all_staging_tables: List[str] = eval(config['STAGING_TABLES']['TABLE_NAMES_LIST'])
    database: str = config['CLUSTER']['DB_NAME']
    
    result: Dict[str, bool] = check_redshift_table_exists(all_staging_tables, database, cur)
    
    tables_to_truncate: List[Any] = []
        
    for key, value in result.items():
        # If this is the first pipeline run, skip truncate step - otherwise it will cause an error
        if value == False:
            logger.warn(f'The table: {key} does not exist in the schema yet. Skipping truncate step for this table.')
        else:
            tables_to_truncate.append(key)
    
    # Truncate existing tables only
    if len(tables_to_truncate) > 0:
    
        truncate_table(tables_to_truncate, database, cur, logger)

    logger.warn('Completed cleanup of staging environment in Redshift.')

    # Close Redshift connection
    connection.close()
    
    
def _create_staging_tables(config, logger, sql_file_path: str = '/home/workspace/racing_pipeline/sql/create_staging_tables.sql') -> None:
    
    logger.warn('About to create staging tables.')
    
    # Establish database connection
    connection = get_redshift_connection(config, logger)
    cur = connection.cursor()
    
    # Create staging tables
    execute_sql_file(sql_file_path, cur, logger)
    
    # Validate that the staging tables now exist and are empty before loading
    logger.warn('Validating that staging tables are empty before loading.')
    
    validation_dict: Dict[str, int] = {'staging_conditions': 0,
                                       'staging_forms': 0,
                                       'staging_horse_sexes': 0, 
                                       'staging_horses': 0,
                                       'staging_markets': 0,
                                       'staging_odds': 0,
                                       'staging_riders': 0,
                                       'staging_runners': 0,
                                       'staging_weather': 0}
                            
    count_table_rows(validation_dict, cur, logger)
    
    logger.warn('Completed creating staging tables.')
    
    connection.close()

    
    
def _load_csv_to_staging_tables(config, logger) -> None:
    """
    Runs SQL query
    """
    logger.warn('Starting load of CSVs into staging tables in Redshift.')
    
    connection = get_redshift_connection(config, logger)
    cur = connection.cursor()
 
    # Copy data from S3 to Redshift
    execute_sql_file('/home/workspace/racing_pipeline/sql/copy_to_staging_tables.sql', cur, logger)

    # Validate that tables contain expected number of rows
    logger.warn('Validating that staging tables have been correctly loaded.')

    validation_dict: Dict[str, int] = {'staging_conditions': 12,
                                       'staging_forms': 43293,
                                       'staging_horse_sexes': 6, 
                                       'staging_horses': 14113,
                                       'staging_markets':3316,
                                       'staging_odds': 410023,
                                       'staging_riders': 1025,
                                       'staging_runners': 44428,
                                       'staging_weather': 4}
                            
    count_table_rows(validation_dict, cur, logger)
    
    logger.warn('Loading CSVs to staging tables in Redshift complete.')

    # Close Redshift connection
    connection.close()
    

def _retrieve_json_from_s3(config, logger) -> bytes:
    """
    Retrieves a file from an Amazon S3 bucket, and returns the file as a bytes object.
    
    Bucket name, file name, and AWS keys are read from a configuration file, passed in
    as a config object.
    
    -- config - configparser config object
    -- logger - logging object

    """
    
    logger.warn('About to retrieve json file from S3.')
    
    # Retrieve tips JSON file from S3 bucket
    s3 = boto3.resource('s3',
                        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'])

    bucket = s3.Bucket(config['S3']['BUCKET_NAME'])
    obj = bucket.Object(key=config['S3']['FILE_TO_RETRIEVE'])
    response = obj.get()
    lines: bytes = response[u'Body'].read()

    # Check that data was returned
    assert lines is not None
    
    logger.warn('Successfully retrieved json file from S3.')

    return lines



def _process_json_for_staging(json_string: bytes, record_length: int, logger) -> List[Dict[str, Any]]:
    """
    Takes a bytes string containing JSON formatted data, and transforms this into a list of
    dictionaries, one per record.
    
    -- record_length - specifies the length of a single record, indicating where each
    dictionary ends.
    -- json_string - JSON formatted data as a string.
    -- logger - logging object.
    """

    logger.warn('About to pre-process JSON file string.')

    # Remove unwanted characters and split string into list
    cleaned_list: List[str] = [e
                          .replace('{', '')
                          .replace('}', '')
                          .replace("'", "")
                          .strip('\\r\\n')
                          .replace("\\r\\n", "")
                          .strip(' ')
                          .split(' : ')[0] for e in str(json_string).split(',')]
    # Separate key: value pairs into individual elements of the list   
    separated_list: List[str] = [e
                            .replace('"', '')
                            .replace(' ', '')
                            .split(':') for e in cleaned_list]
        
    # Remove the first and last records of the list - these feature junk characters and data
    list_of_elements: List[str] = separated_list[record_length:-record_length]
        
    # Split the list into individual records, and append records as dictionaries to new list
    dict_list: List[Any] = []
    counter: int = 0
        
    for num in range(0, len(list_of_elements), record_length):
        if counter == 0:
            start: int = 0
            stop: int = record_length
            counter: int = counter + 1
        else:
            start: int = start + record_length
            stop: int = stop + record_length
        current_record: List[List[Any]] = list_of_elements[start: stop]    
        dict_list.append({item[0]: item[-1] for item in current_record})
    
    # Check that the number of records is as expected
    assert len(dict_list) == 38247, f"Actual length of dict_list: {len(dict_list)}, expected length: {38247}"
        
    logger.warn('Completed pre-processing of JSON file string.')
        
    return dict_list


def staging(config, logger) -> Dict[str, Any]:
    """
    Orchestrates execution of staging functions.
    """
    # Prep staging tables - empty if contain data
    _cleanup_staging_environment(config, logger)
    
    # Create staging tables
    _create_staging_tables(config, logger)
    
    # Load CSV files from S3 to staging tables
    _load_csv_to_staging_tables(config, logger)
    
    # Retrieve and process JSON data from S3 - for later conversion to DataFrame
    bytes_json: bytes = _retrieve_json_from_s3(config, logger)
        
    # Process JSON to convert to usable dict structure
    json_dict_list: Dict[str, Any] = _process_json_for_staging(bytes_json, 9, logger)
        
    return json_dict_list
