import pandas as pd 
import psycopg2
import json

def read_sql_to_df(sql):
    """Internal: execute sql and get dataframe as a return 
    
    Args:
        sql (str): sql to be executed 
        db (psycopg2.connect): connect to ciq target database 
        cursor (database connection.cursor): 
    
    Returns:
        pd.DataFrame: contains result for executed sql 
    """
    connect_info = json.load(open('cfg/cn_copy.json', 'r'))
    connection = get_connection(connect_info)
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(data,columns = columns)
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        print("Error: %s" % error)


def get_connection(dbInfo):
    """get the db connection using info in dbInfo json file
    
    Args:
        dbInfo (.json): user-pw for ciq target database 
    
    Returns:
        psycopg2.connect
    """
    # config parameters of db connection

    db = psycopg2.connect(
        host=dbInfo['host'],
        database=dbInfo["dbname"],
        user=dbInfo["user"],
        password=dbInfo["pwd"],
        port=dbInfo['port'])
    

    print('DB connection set up!\n')
    return db


def estimatesQ(ls_ids, dataitemids, datestart):
    sql= f"""
    CREATE TEMP TABLE PC AS
         SELECT EP.*,
                EC.tradingitemid,
                EC.estimateConsensusId
         FROM ciqEstimatePeriod EP
                JOIN ciqEstimateConsensus   EC ON EC.estimatePeriodId = EP.estimatePeriodId
         WHERE EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
               AND EP.periodTypeId = 2 -- Quarter 
               AND EP.periodenddate > '{datestart}';
    CREATE INDEX idx_pc ON PC (estimateConsensusId);

    SELECT
           PC.*,
           D.*
           ED.dataitemid,
           ED.currencyId,
           ED.dataItemValue,
           ED.effectiveDate,
           ED.toDate,
           ED.estimatescaleid
    FROM ciqEstimateNumericData ED
       JOIN ciqDataitem D on D.dataitemid = ED.dataItemId -- explanational table
        JOIN PC ON ED.estimateConsensusId = PC.estimateConsensusId
    WHERE ED.dataItemId IN ({', '.join([str(id) for id in dataitemids])});
    """
    return read_sql_to_df(sql)
