from typing import List, Dict


def check_redshift_table_exists(table_list: List[str], schema: str, cursor) -> Dict[str, bool]:
    """
    Executes a SELECT EXISTS sql query against the svv_table_info of a Redshift
    database, returning a dict with either True or False indicating the prescence or
    abscence of each table checked for.
    
    """
    result_list: List = []
        
    for table in table_list:
        
        query: str = f"""
        SELECT EXISTS (
        SELECT 1 
        FROM svv_table_info
        WHERE "database" = '{schema}'
        AND "table" = '{table}')"""
        
        cursor.execute(query)
        result = cursor.fetchall()
        
        if result[0][0] == False:
            result_list.append(False)
        else:
            result_list.append(True)
            
    results: Dict[str, bool] = dict(zip(table_list, result_list)) 
    
    return results
