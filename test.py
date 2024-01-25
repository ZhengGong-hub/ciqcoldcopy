
import pandas as pd
from queries.query_1_sql import query_1_sql
import json
from queries.query_2_sql import estimatesQ
# from query_3_sql import query_3_sql, query_3_sql_2, query_3_sql_3


def query_1(companyids):
    calyears = [2019, 2020, 2021, 2022, 2023]
    return query_1_sql(companyids, calyears)

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
    companyids = pd.read_csv('/Users/zhenggong/ec_oa_research/symbol_companyid_universe.csv', index_col=0)['companyid'].dropna().astype(int)# [:1000]
    companyids = [24937, ]

    # functions 
    #   
    # dataitemids = [100176, 100177, 100178, 22315, 22316, 22317, 22318, 22319, 22320, 22322, 22323, 22325, 22326]
    dataitemids = [100176, 100177, 100178]
    datestart = '2023-01-01'

    df = estimatesQ(companyids, dataitemids, datestart)
    df.to_csv('data/query2.csv')
    print(df)
        
