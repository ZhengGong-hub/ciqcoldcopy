# gz sep10
# https://quant.stackexchange.com/questions/10048/whats-the-meaning-of-the-intercept-in-asset-pricing-model
import pandas as pd 
import sys
from statsmodels.regression.rolling import RollingOLS

ROOTPATH = '/Users/zhenggong/Documents/Github/ba_thesis/' # for importing and reference management 
sys.path.append(ROOTPATH)
# internal import
from capitaliq.databaseManager import get_hist_miadj_pricing
from gff.gff_function import famaFrench5Factor, momentumFactor



def calculate_fwd_ret(companyid = 32307, addr = 'data/car_data/v1/', start_date = '2018-01-01', end_date = '2023-06-01', rolling_window = 252):
    # calculate CAR cumulative abnormal return
    #   FOR ONE STOCK

    # step 0: paramter declarations & data preparation

    # load in ref_table

    companyid = companyid # nvidia 32307 # please read from ref_table

    start_date, end_date = start_date, end_date

    # rolling_window = 252 # 252 trading days a year 
    addr = addr

    # step 1: pull out daily return of one individual stock
    price = get_hist_miadj_pricing(start_date, end_date, [companyid, ])

    if len(price) <= rolling_window:
        return 1 # price history too short 

    price['divadjopen'] = price['priceopen'] * price['divadjfactor']
    price['divadjclose'] = price['divadjclose'].astype(float)
    price['pricedate'] = pd.to_datetime(price['pricedate'])
    price = price[['divadjopen', 'divadjclose', 'pricedate']]
    # print(price)

    # step 2: pull out market data
    sp_500 = pd.read_csv('data/index/SPY.csv')
    sp_500['discount_factor'] = sp_500['Adj Close']/sp_500['Close']
    sp_500['sp500_adjclose'] = sp_500['Close'] * sp_500['discount_factor']
    sp_500['sp500_adjopen'] = sp_500['Open'] * sp_500['discount_factor']
    sp_500.rename(columns={'Date':'pricedate'}, inplace = True)
    sp_500['pricedate'] = pd.to_datetime(sp_500['pricedate'])
    # print(sp_500)

    # attach factors on price_df
    price = pd.merge(price, sp_500[['sp500_adjclose', 'sp500_adjopen', 'pricedate']], on = 'pricedate', how = 'inner')
    # print(price)

    price['stock_ret_open'] = price['divadjopen'].pct_change().shift(-1)
    price['stock_ret_close'] = price['divadjclose'].pct_change().shift(-1)
    price['sp500_ret_open'] = price['sp500_adjopen'].pct_change().shift(-1)
    price['sp500_ret_close'] = price['sp500_adjclose'].pct_change().shift(-1)
    # print(price)
    # assert False

    price.to_parquet(addr + f'{companyid}.parquet')

    return 0

import pandas as pd 
import sys 
import os
import tqdm



# internal
from gff.gff_function import famaFrench5Factor, momentumFactor
from capitaliq.databaseManager import get_hist_miadj_pricing
from car.calc_et_car import calc_et_car

from car.calc_car import calculate_car

if __name__ == "__main__":

    universe = pd.read_csv('data/processed/et_ref_COMPLETE_Big500_2012_2022.csv', index_col = [0])

    # print(ref)
    for companyid in tqdm.tqdm(universe['companyid'].unique()):
        # print(companyid)
        if not os.path.exists(f'data/fwd_ret_data/v1/{companyid}.parquet'):
            calculate_fwd_ret(companyid, addr='data/fwd_ret_data/v1/', start_date='2010-01-01', end_date='2023-06-01')
            print(f'{companyid} is done!')


    