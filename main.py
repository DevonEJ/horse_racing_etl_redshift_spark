import logging
import configparser
import json
from typing import Any, List, Dict


# Import configuration creator
from utils.config import create_configuration_object

# Import spark session utility
from utils.spark_connection import create_spark_session

# Import pipeline stages
from pipeline.staging_functions import staging
from pipeline.transform_functions import transform
from pipeline.load_analytics_tables import load


# Set up logging for pipeline
log = logging.getLogger('pipeline_logger')

# Create config dict
config = create_configuration_object('/home/workspace/racing_pipeline/config/configuration.cfg')

# Create spark session
spark = create_spark_session()


# Execute pipeline stages
def main(configuration, logging_object) -> None:
    
    logging_object.warn('Start of pipeline.')

    # Stage data to Staging Tables
    tips_data_dict: Dict[str, Any] = staging(configuration, logging_object)
        
    # Transform dataframes
    df1, df2, df3 = transform(spark, configuration, logging_object, tips_data_dict)
    
    # Load Analytics tables
    load(df1, df2, df3, configuration, logging_object)
    
    logging_object.warn('End of pipeline.')



if __name__ == "__main__":
    main(config, log)
