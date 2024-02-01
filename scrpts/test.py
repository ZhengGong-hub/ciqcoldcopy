
import pandas as pd
import json
import os 
import sys
sys.path.append('../queries')
from querymanager import estimatesQ, get_transcript, get_person

if __name__ == "__main__":

    # get companyids
    companyids = pd.read_csv('/Users/zhenggong/ec_oa_research/symbol_companyid_universe.csv', index_col=0)['companyid'].dropna().astype(int)# [:1000]
    companyids = [24937, ]

    # functions 

    # get estimate   
    # dataitemids = [100176, 100177, 100178, 22315, 22316, 22317, 22318, 22319, 22320, 22322, 22323, 22325, 22326]
    dataitemids = [100176, 100177, 100178]
    datestart = '2023-01-01'

    df = estimatesQ(companyids, dataitemids, datestart)
    df.to_csv('../data/query_estimates.csv')
    print(df)


    # get transcript
    df = get_transcript(ls_ids = companyids, startdatestr = '2023-01-01', enddatestr='2025-01-01')
    df.to_csv('../data/query_trasncript.csv')
    print(df)
        
    # get person details 
    df = get_person(person_ids = [169600, ])
    df.to_csv('../data/query_person.csv')
    print(df)

    