import psycopg2
from typing import Dict, Any


def get_redshift_connection(config: Dict, logger):
    """
    Returns a connection to a Redshift database hosted in AWS, when provided with a path to 
    configuration file holding the host, database name, username, password and port of the target
    database/cluster.
    """
    conn_str = "host={host} dbname={db} user={user} password={paswd} port={port}".format(
        host=config['CLUSTER']['HOST'],
        db=config['CLUSTER']['DB_NAME'],
        user=config['CLUSTER']['DB_USER'],
        paswd=config['CLUSTER']['DB_PASSWORD'],
        port=config['CLUSTER']['DB_PORT']
    )
                                                                     
    conn = psycopg2.connect(conn_str)
                            
    conn.autocommit = True
                            
    logger.warn('Redshift connection has been successfully established.')
                            
    return conn
