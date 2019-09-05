from typing import List


def truncate_table(table_names: List[str], database_name: str, cursor, logger) -> None:
    """
    Truncates a given table name against the database connection associated with the cursor object specified
    as an argument.
    """
    for table in table_names:
                
        logger.warn(f'About to truncate table: {table} from database: {database_name}.')
        
        query = f"""TRUNCATE TABLE {table.strip()}"""
        
        cursor.execute(query)
        
        logger.warn(f'Truncate table query executed successfully on table: {table}.')