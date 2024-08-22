import pandas as pd 
import sys 
import os
import tqdm
import math
import numpy as np

ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal

def get_earning_release_date(universe: pd.DataFrame, earning_release_date_addr:str) -> pd.DataFrame:
    if os.path.exists(earning_release_date_addr):
        earning_release_date = pd.read_csv(earning_release_date_addr, index_col = [0])
        earning_release_date['earningsdate'] = pd.to_datetime(earning_release_date['earningsdate'])
        earning_release_date['earningsdate-5days'] = pd.to_datetime(earning_release_date['earningsdate-5days'])
        earning_release_date['last_earnings+5days'] = pd.to_datetime(earning_release_date['last_earnings+5days'])
        return earning_release_date
    
    # earnings release date
    EPS_addr = 'data/EPS.csv' # the other option is 1275, refer to tot_equity.csv
    EPSnorm_addr = 'data/EPSnormalized.csv' # the other option is 1275, refer to tot_equity.csv
    rev_addr = 'data/revenue.csv' # the other option is 1275, refer to tot_equity.csv

    if os.path.exists(EPS_addr) and os.path.exists(EPSnorm_addr) and os.path.exists(rev_addr):
        earning_release_date = pd.read_csv(EPS_addr, index_col = [0])[['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate', 'effectivedate']]
        earning_release_date.sort_values(by = ['companyid', 'periodenddate', 'effectivedate'], inplace = True)
        earning_release_date.drop_duplicates(subset=['companyid', 'effectivedate', 'periodenddate'], keep='first', inplace=True)
        earning_release_date.rename(columns={'effectivedate':'earningsdate'}, inplace=True)

        EPSnorm = pd.read_csv(EPSnorm_addr, index_col = [0])[['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate', 'effectivedate']]
        EPSnorm.sort_values(by = ['companyid', 'periodenddate', 'effectivedate'], inplace = True)
        EPSnorm.drop_duplicates(subset=['companyid', 'effectivedate', 'periodenddate'], keep='first', inplace=True)
        earning_release_date = pd.merge(earning_release_date, EPSnorm[['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate'], how = 'outer')
        earning_release_date['earningsdate'] = earning_release_date['earningsdate'].fillna(earning_release_date['effectivedate'])
        earning_release_date.drop(columns = ['effectivedate'], inplace = True)

        rev = pd.read_csv(rev_addr, index_col = [0])[['companyid', 'periodenddate', 'fiscalyear', 'fiscalquarter', 'effectivedate']]
        rev.sort_values(by = ['companyid', 'periodenddate', 'effectivedate'], inplace = True)
        rev.drop_duplicates(subset=['companyid', 'effectivedate', 'periodenddate'], keep='first', inplace=True)
        earning_release_date = pd.merge(earning_release_date, rev[['companyid', 'periodenddate', 'fiscalyear', 'fiscalquarter', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate'], how = 'outer')
        earning_release_date['earningsdate'] = earning_release_date['earningsdate'].fillna(earning_release_date['effectivedate'])
        earning_release_date.drop(columns = ['effectivedate'], inplace = True)

        earning_release_date.drop_duplicates(subset=['companyid', 'earningsdate', 'periodenddate'], keep='first', inplace=True)

    else:
        raise ValueError('EPS.csv, EPSnormalized.csv, revenue.csv not found, FETCH from database!')
        
    earning_release_date['earningsdate'] = pd.to_datetime(earning_release_date['earningsdate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.tz_localize(None) # convert to eastern time
    earning_release_date['periodenddate'] = pd.to_datetime(earning_release_date['periodenddate'])
    earning_release_date['earningsdate_delay'] = (earning_release_date['earningsdate'] - earning_release_date['periodenddate']).dt.days
    earning_release_date.sort_values(by = ['companyid', 'periodenddate', 'earningsdate'], inplace = True)
    earning_release_date.drop_duplicates(subset = ['companyid', 'fiscalyear', 'fiscalquarter', 'periodenddate'], keep = 'first', inplace = True)
    earning_release_date = earning_release_date.query('earningsdate_delay < 90')

    # manipulate
    earning_release_date['last_earningsdate'] = earning_release_date['earningsdate'].shift(1)
    earning_release_date['last_earningsdate_distance'] = (earning_release_date['earningsdate'] - earning_release_date['last_earningsdate']).dt.days
    earning_release_date = earning_release_date.query('30 <= last_earningsdate_distance <= 150')
    earning_release_date['earningsdate-5days'] = earning_release_date['earningsdate'] - pd.Timedelta(days=5)
    earning_release_date['last_earnings+5days'] = earning_release_date['last_earningsdate'] + pd.Timedelta(days=5)
    # print(earning_release_date)

    earning_release_date = earning_release_date.drop(columns=['last_earningsdate_distance', 'earningsdate_delay'])
    os.makedirs(os.path.dirname(earning_release_date_addr), exist_ok=True)
    earning_release_date.to_csv(earning_release_date_addr)
    print('earning_release_date info FINISHED')
    return earning_release_date