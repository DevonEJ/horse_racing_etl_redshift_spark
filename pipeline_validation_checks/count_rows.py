from typing import Dict


def count_table_rows(expected_dict: Dict[str, int], cursor, logger) -> None:
    """
    Takes a dict of table names, and expected row counts, and checks that each table has the
    expected number of rows.
    """
    for table, row_count in expected_dict.items():
        
        sql: str =  f"""SELECT COUNT(*) FROM {table}"""
            
        cursor.execute(sql)
        result = cursor.fetchone()
        
        actual_count: str = result[0]
            
        logger.warn(f'Checking row count for {table} - expected is {row_count}, actual is {actual_count}.')
        
        assert actual_count == row_count, "Failed validation check: count_table_rows"
