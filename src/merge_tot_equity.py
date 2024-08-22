import pandas as pd 
import sys 
import os
import tqdm
import math
import numpy as np
import os

ROOTPATH = '/Users/zhenggong/Documents/Github/ba_thesis/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal
from capitaliq.databaseManager import get_historical_fundamental 

def merge_tot_equity(universe: pd.DataFrame, tot_equity_PATH:str) -> pd.DataFrame:
    # -------- add tot_equity, btm --------- #
    # total equity (as reported) (by million)
    if os.path.exists(tot_equity_PATH):
        tot_equity = pd.read_csv(tot_equity_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(tot_equity_PATH), exist_ok=True)
        tot_equity = get_historical_fundamental(ls_ids=universe['companyid'].values, periodtypeid=[2], ls_dataitemid=[48859, ], startdate='2000-01-01', startyear= 2000) # the other option is 1275
        tot_equity.to_csv(tot_equity_PATH)
    # print(tot_equity)
    tot_equity.drop_duplicates(subset = ['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue'], keep = 'first', inplace = True) 
    tot_equity = tot_equity[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue']].groupby(['companyid', 'fiscalyear', 'fiscalquarter']).mean().reset_index()
    tot_equity.rename(columns = {'dataitemvalue': 'tot_equity'}, inplace=True)
    # print(tot_equity)

    universe = pd.merge(universe, tot_equity[['companyid', 'tot_equity', 'fiscalyear', 'fiscalquarter']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    print('tot_equity info FINISHED')
    return universe