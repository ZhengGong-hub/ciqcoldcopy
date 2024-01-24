
import pandas as pd
from dbconnect import get_connection, read_sql_to_df
from query_1_sql import query_1_sql
import json
# from query_2_sql import query_2_sql, query_2_sql_2, query_2_sql_3
# from query_3_sql import query_3_sql, query_3_sql_2, query_3_sql_3

# config parameters of db connection
db_connect_information1 = json.load(open('cfg/cn_copy.json', 'r'))

db_connect_information2 = json.load(open('cfg/aws.json', 'r'))

db_connect_information = {
            'yun' : db_connect_information1,
            'tyl' : db_connect_information2,
        }

companyid_file = "symbol_companyid_universe.csv"

def query_1(companyids):
    calyears = [2019, 2020, 2021, 2022, 2023]
    return query_1_sql(companyids, calyears)

def query_2(companyids, flag=False):
    observeDate = '2024-01-12'
    dataitemids = [100176, 100177, 100178, 22315, 22316, 22317, 22318, 22319, 22320, 22322, 22323, 22325, 22326]
    dataitemids = [100176, 100177, 100178]
    datestart = '2023-01-01'
    if flag == 3:
        return query_2_sql_3(observeDate, companyids, dataitemids, datestart)
    elif flag:
        return query_2_sql_2(observeDate, companyids, dataitemids, datestart)
    else:
        return query_2_sql(observeDate, companyids, dataitemids, datestart)

def query_2_2(companyids):
    return query_2(companyids, flag=True)

def query_2_3(companyids):
    return query_2(companyids, flag=3)

def query_3(companyids, flag=False):
    start, end = '2023-01-01 00:00:00', '2023-12-31 23:59:59'
    if flag == 3:
        return query_3_sql_3(companyids, start, end)
    elif flag:
        return query_3_sql_2(companyids, start, end)
    else:
        return query_3_sql(companyids, start, end)

def query_3_2(companyids):
    return query_3(companyids, flag=True)

def query_3_3(companyids):
    return query_3(companyids, flag=3)

if __name__ == "__main__":

    # get companyids
    companyids = pd.read_csv(companyid_file, index_col=0)['companyid'].dropna().astype(int)[:10000]

    for query_info, query_func in [
            ['query 1  ', query_1  ],
            # ['query 2  ', query_2  ],
            # ['query 2-2', query_2_2],
            #['query 2-3', query_2_3],
            #['query 3  ', query_3  ],
            #['query 3-2', query_3_2],
            # ['query 3-3', query_3_3],
        ]:
        connect_info = db_connect_information['yun']
        connection = get_connection(connect_info)
        cursor = connection.cursor()
        print(f'site: yun {query_info} ', end='', flush=True)
        
        sql = query_func(companyids)
        #print(sql)
        start_time = pd.Timestamp.now()
        print(f'From {start_time} ', end='', flush=True)
        df = read_sql_to_df(sql, db=connection, cursor=cursor)
        end_time = pd.Timestamp.now()
        connection.close()
        print(df)
        #df.to_csv('test.csv')
        #print(pd.Timestamp.now())
