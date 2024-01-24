import pandas as pd 
import psycopg2
import json

def read_sql_to_df(sql, db, cursor):
    """Internal: execute sql and get dataframe as a return 
    
    Args:
        sql (str): sql to be executed 
        db (psycopg2.connect): connect to ciq target database 
        cursor (database connection.cursor): 
    
    Returns:
        pd.DataFrame: contains result for executed sql 
    """
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(data,columns = columns)
    except (Exception, psycopg2.DatabaseError) as error:
        db.rollback()
        print("Error: %s" % error)


def get_connection(dbInfo):
    """get the db connection using info in dbInfo json file
    
    Args:
        dbInfo (.json): user-pw for ciq target database 
    
    Returns:
        psycopg2.connect
    """

    db = psycopg2.connect(
        host=dbInfo['host'],
        database=dbInfo["dbname"],
        user=dbInfo["user"],
        password=dbInfo["pwd"],
        port=dbInfo['port'])
    return db