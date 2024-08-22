import pandas as pd 
import sys 
import os
import tqdm
import math
import numpy as np

ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal
from capitaliq.databaseManager import get_cur_mc_global 


def merge_marketcap(universe: pd.DataFrame, marketcap_PATH:str) -> pd.DataFrame:
    # -------- add market cap --------- #
    if os.path.exists(marketcap_PATH):
        mc = pd.read_csv(marketcap_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(marketcap_PATH), exist_ok=True)
        mc = get_historical_marketcap(et_ref['companyid'].unique(), '2000-01-01', '2066-01-01')
        mc.to_csv(marketcap_PATH)

    mc = mc.query('marketcap > 0')
    mc['pricingdate'] = pd.to_datetime(mc['pricingdate']) + pd.Timedelta(days=1) # shift one day to avoid future leakage
    mc.sort_values(by = ['companyid', 'pricingdate'], inplace = True)
    mc['log_marketcap'] = mc['marketcap'].apply(lambda x: math.log(x))

    universe = pd.merge(universe, mc[['companyid', 'pricingdate', 'marketcap', 'log_marketcap']], left_on=['companyid', 'ec_et_day'], right_on=['companyid', 'pricingdate'], how = 'left')
    print('marketcap info FINISHED')
    return universe