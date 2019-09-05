import logging
import pyspark
import configparser
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from typing import Any, Dict, List
from pyspark.sql.functions import *

# Import relevant utils
from utils.connections import get_redshift_connection

# Import PySpark DataFrame schemas
from pipeline.staging_dataframe_schemas import (
    staging_riders_schema,
    staging_weather_schema,
    staging_horses_schema,
    staging_horse_sexes_schema,
    staging_conditions_schema,
    staging_forms_schema,
    staging_odds_schema,
    staging_runners_schema,
    staging_markets_schema
)


def _redshift_to_spark_dataframe(table_name: str, cursor, connect, logger, spark_session, schema = None, row_check: int = None) -> pyspark.sql.DataFrame:
    """
    Reads redshift table into a Pandas DataFrame, checks that it has the expected number of rows (if argument passed in), 
    and converts to a Spark DataFrame.
    
    -- table_name - str, name of table to return as Dataframe.
    -- cusor - psycopg2 database connection cursor.
    -- connect - psycopg2 datbase connection object.
    -- row_check - specify expected number of rows in DataFrame, to be asserted - optional.
    """
    logger.warn(f'Creating DataFrame for table: {table_name}')
    
    sql_query = f"""SELECT * FROM {table_name}"""
    
    # Retrieve Redshift table as pandas dataframe
    pandas_df =  psql.read_sql(sql_query, connect, coerce_float = False)
        
    # Check dataframe has expected number of rows
    if row_check is not None:
        assert len(pandas_df) == row_check, f"The dataframe for the table: {table_name} has {len(pandas_df)} rows, expected {row_check} rows."
    
    # Convert pandas dataframe to Spark dataframe - pass in relevant dataframe schema
    pyspark_df = spark_session.createDataFrame(pandas_df, schema)
    
    return pyspark_df


def _json_dict_to_spark_dataframe(json_dict: Dict[str, Any], spark_session, logger) -> pyspark.sql.DataFrame:
    """
    Takes a dictionary object, featuring JSON data, and converts this to an RDD and then returns as a PySpark
    DataFrame.
    
    """
    logger.warn(f'Creating DataFrame for JSON file: staging_tips')
    
    # Create a spark context from the spark session object, for working with RDDs
    spark_context = spark_session.sparkContext
    
    # First convert dict object to an RDD, then convert RDD to DataFrame
    rdd = spark_context.parallelize([json_dict])
    df = spark_session.read.json(rdd)
    
    return df



def _join_pyspark_dfs(logger,
                      dfs: List[pyspark.sql.DataFrame],
                      df_names: List[str],
                      join_cols: Dict[str, str],
                      join_type: str,
                      expected_rows: int = None,
                      cols_to_exclude: List[str] = None) -> pyspark.sql.DataFrame:
    """
    Performs a join on pyspark dataframes, providing options to verify row count afterwards, and drop unwanted columns, before 
    returning the joined dataframe. By default, all unique columns from both dataframes are retained.. If both dataframes have a
    column with the same name, the left-hand df's version is chosen.
    
    logger - Logging object.
    dfs - List of dataframes to be joined.
    df_names - Names of dataframes to be joined, as strings, in same order as previous list.
    join_cols - Dict with keys 'left' and 'right' indicating the relative names of the joining columns as strings.
    join_type - String indicating desired join.
    expected_rows - Optional - post-join row count verification.
    cols_to_exclude - Optional - list of columns to exclude from result, as strings.
    
    """
    logger.warn(f'About to join dataframes: {df_names[0]} and {df_names[1]}')
    left = join_cols['left']
    right = join_cols['right']
    
    # Join dataframes and retain columns of each, only dropping the right-hand key column
    interim_df = dfs[0].alias('a').join(dfs[1].alias('b'), col(f"a.{left}") == col(f"b.{right}"), how = join_type)\
    .select(
        [col('a.'+ x) for x in dfs[0].columns]
        + 
        # Ensure no duplicate columns - only add cols from right-hand df that do not exist in the left already
        [col('b.'+ x) for x in dfs[1].columns])
    
    # Check that joined df has expected number of rows
    if expected_rows is not None:
        assert interim_df.count() == expected_rows, f"Join of dataframes: {df_names[0]} and {df_names[1]} has resulted in dataframe with rows: expected {expected_rows} and actual {interim_df.count()}"
    else:
        logger.warn('No row count validation performed on joined dataframe.')
        
    if cols_to_exclude is not None:
        # Drop unwanted columns, if specified
        final_df = interim_df.drop(*cols_to_exclude)
        
        return final_df
        
    return interim_df


    

# Check for and remove null values
# Remove useless columns - e.g. 'flight//' in tips dataframe
# Change data types to be correct
# Make all datetimes human-readable format
# Standardise column names - e.g. all lower case
# How can the tips dataset merge into the others?
# Any feature engineering needed? E.g. for creation of reports for a BI team
# -- Check the notebook for any tips -- 
    
    
    
    
def transform(spark_session, config, logger, json_data: Dict[str, Any]) -> List[pyspark.sql.DataFrame]:
    """
    Orchestrates execution of transformation functions.
    
    """
    # Establish database connection
    connection = get_redshift_connection(config, logger)
    cur = connection.cursor()
    
    # Read staging Redshift tables into spark dataframes
    staging_horses_df = _redshift_to_spark_dataframe('staging_horses', cur, connection, logger, spark_session, schema = staging_horses_schema(), row_check = 14113)
    
    staging_markets_df = _redshift_to_spark_dataframe('staging_markets', cur, connection, logger, spark_session, schema = staging_markets_schema(), row_check = 3316)
    
    staging_forms_df = _redshift_to_spark_dataframe('staging_forms', cur, connection, logger, spark_session, schema = staging_forms_schema(), row_check = 43293)
    
    staging_horse_sexes_df = _redshift_to_spark_dataframe('staging_horse_sexes', cur, connection, logger, spark_session, schema = staging_horse_sexes_schema(), row_check = 6)
    
    staging_odds_df = _redshift_to_spark_dataframe('staging_odds', cur, connection, logger, spark_session, schema = staging_odds_schema(), row_check = 410023)
    
    staging_riders_df = _redshift_to_spark_dataframe('staging_riders', cur, connection, logger, spark_session, schema = staging_riders_schema(), row_check = 1025)
    
    staging_runners_df = _redshift_to_spark_dataframe('staging_runners', cur, connection, logger, spark_session, schema = staging_runners_schema(), row_check = 44428)
    
    staging_conditions_df = _redshift_to_spark_dataframe('staging_conditions', cur, connection, logger, spark_session, schema = staging_conditions_schema(), row_check = 12)
    
    staging_weather_df = _redshift_to_spark_dataframe('staging_weather', cur, connection, logger, spark_session, schema = staging_weather_schema(),  row_check = 4)
    
    
    # Read JSON file (as dict) into spark dataframe
    staging_tips_df = _json_dict_to_spark_dataframe(json_data, spark_session, logger)
    
    
    ## Denormalise the datasets by joining on relevant keys, to create analytics tables
    # Create Horses Analytics Table
    horses_df_stage_0 = _join_pyspark_dfs(logger,
                                          dfs = [staging_runners_df, staging_horses_df],
                                          df_names = ['staging_runners_df', 'staging_horses_df'],
                                          join_cols = {'left': 'horse_id', 'right': 'id'},
                                          join_type = 'full',
                                          cols_to_exclude = ['id', 'dam_id', 'sire_id', 'trainer_id']
                                         )
    
    # Change the name of the 'age' column in horses_df to clarify meaning
    horses_df_stage_0_updated = horses_df_stage_0.withColumnRenamed("age", "horse_age")
    
    
    horses_df_stage_1 = _join_pyspark_dfs(logger,
                                          dfs = [horses_df_stage_0_updated, staging_horse_sexes_df],
                                          df_names = ['horses_df_stage_0_updated', 'staging_horse_sexes_df'],
                                          join_cols = {'left': 'sex_id', 'right': 'id'},
                                          join_type = 'full',
                                          cols_to_exclude = ['id', 'sex_id', 'rider_id']
                                         )
    
    # Change the name of the 'sex' column in horse_sexes_df to clarify meaning
    horses_df_stage_0_updated = horses_df_stage_0.withColumnRenamed("name", "horse_sex")
    
        
    # Create Races Analytics table
    races_stage_0_df = _join_pyspark_dfs(logger,
                                          dfs = [staging_forms_df, staging_markets_df],
                                          df_names = ['staging_forms_df', 'staging_markets_df'],
                                          join_cols = {'left': 'market_id', 'right': 'id'},
                                          join_type = 'right',
                                          cols_to_exclude = ['collected', 'id']
                                         )
    
    races_stage_1_df = _join_pyspark_dfs(logger,
                                          dfs = [races_stage_0_df, staging_weather_df],
                                          df_names = ['races_stage_0_df', 'staging_weather_df'],
                                          join_cols = {'left': 'weather_id', 'right': 'id'},
                                          join_type = 'left',
                                          cols_to_exclude = ['id']
                                         )
    
    # Change name of 'name' column from weather df to clarify meaning
    races_stage_1_df_updated = races_stage_1_df.withColumnRenamed('name', 'weather')
    
    
    races_stage_2_df = _join_pyspark_dfs(logger,
                                          dfs = [races_stage_1_df_updated, staging_conditions_df],
                                          df_names = ['races_stage_1_df_updated', 'staging_conditions_df'],
                                          join_cols = {'left': 'condition_id', 'right': 'id'},
                                          join_type = 'left',
                                          cols_to_exclude = ['id']
                                         )
    
    # Change name of 'name' column from conditions df to clarify meaning
    races_stage_2_df_updated = races_stage_2_df.withColumnRenamed('name', 'conditions')

    ## Process Tips dataframe
    # remove unwanted columns to product Customers table
    # Filter to only show active customers
    staging_tips_interim = staging_tips_df.select('ID',
                                                  'BetType',
                                                  'Date',
                                                  'Tipster',
                                                  'Track',
                                                  'TipsterActive',
                                                  'Odds',
                                                  'Result'
                                                 ).filter(col('TipsterActive') == True)
    
    # Make all Customer table column names lower case
    staging_tips_df_updated = staging_tips_interim.toDF(*[c.lower() for c in staging_tips_interim.columns])
    
    connection.close()
    
    return staging_tips_df_updated, races_stage_2_df_updated, horses_df_stage_0_updated
