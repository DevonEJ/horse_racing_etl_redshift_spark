import logging

def execute_sql_file(file_path: str, cursor, logger) -> None:
    """
    Takes a path to an executable SQL file, and executes the file against a database.
    
    """
    sql_file = open(file_path, 'r')
    cursor.execute(sql_file.read())
    file_run = file_path.split('/')[-1]
    sql_file.close()
    logger.warn(f'SQL file: {file_run} executed successfully.')
