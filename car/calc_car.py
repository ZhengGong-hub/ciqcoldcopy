# gz sep10
# https://quant.stackexchange.com/questions/10048/whats-the-meaning-of-the-intercept-in-asset-pricing-model
import pandas as pd 
import sys
from statsmodels.regression.rolling import RollingOLS
import tqdm
import os

ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)
# internal import
from capitaliq.databaseManager import get_hist_miadj_pricing
from gff.gff_function import famaFrench5Factor, momentumFactor



def calculate_car(companyid = 32307, addr = 'data/car_data/v1/', start_date = '2018-01-01', end_date = '2030-06-01', rolling_window = 252):
    os.makedirs(addr, exist_ok=True)
    # calculate CAR cumulative abnormal return
    #   FOR ONE STOCK

    # step 0: paramter declarations & data preparation

    # load in ref_table

    companyid = companyid # nvidia 32307 # please read from ref_table

    start_date, end_date = start_date, end_date

    # rolling_window = 252 # 252 trading days a year 
    addr = addr

    # step 0: trading calendar
    tcalendar = pd.read_csv('data/tradingcalendar/tradingcalendar.csv')
    tcalendar['tradingday'] = pd.to_datetime(tcalendar['tradingday'])

    # step 1: pull out daily return of one individual stock
    price = get_hist_miadj_pricing(start_date, end_date, [companyid, ])[['priceopen', 'priceclose', 'divadjclose', 'pricedate']]
    price.dropna(inplace = True)

    if len(price) <= 2 * rolling_window: # just to have a big buffer, otherwise = rolling_window will do
        return 1 # price history too short 


    price['divadjclose'] = price['divadjclose'].astype(float)
    price['pricedate'] = pd.to_datetime(price['pricedate'])

    # line by line, we should not calculate the car if the price is smaller than a number (e.g. 1)
    # let's say, if one day's close price is smaller than 1, we should not calculate the car for the next day 
    #   because the return might be too high and not meaningful
    price['normal_price_flag'] = price['priceclose'] >= 1
    price['normal_price_flag'] = price['normal_price_flag'].shift(1)


    earliest_price_date = price['pricedate'].iloc[0]
    latest_price_date = price['pricedate'].iloc[-1]

    # trim tcalendar based on that 
    tcalendar = tcalendar.query('tradingday >= @earliest_price_date and tradingday <= @latest_price_date')
    price['divadjclose'] = price['divadjclose'].ffill()
    price['stock_ret'] = 100*(price['divadjclose'].pct_change())

    if len(price) <= 1.5 * rolling_window: # just to have a big buffer, otherwise = rolling_window will do
        return 1 # price history too short 

    price = pd.merge(tcalendar, price, left_on = 'tradingday', right_on = 'pricedate', how = 'left')

    # step 2: pull out fama french factors 
    ff_factor = famaFrench5Factor('d')
    ff_factor['date_ff_factors'] = pd.to_datetime(ff_factor['date_ff_factors'])
    ff_factor = ff_factor.rename(columns = {'date_ff_factors':'pricedate'})

    mom_factor = momentumFactor('d')
    mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'])
    mom_factor = mom_factor.rename(columns = {'date_ff_factors':'pricedate'})

    # attach factors on price_df
    price = pd.merge(price, ff_factor, on = 'pricedate', how = 'inner')
    price = pd.merge(price, mom_factor, on = 'pricedate', how = 'inner')
    price['stock_ret-RF'] = price['stock_ret'] - price['RF']

    # step 3: decide rolling window and other parameters 
    rwindow = rolling_window 

    # step 4: run linear regression
    model = RollingOLS(endog =price['stock_ret-RF'].values , exog=price[['Mkt-RF','SMB','HML','RMW', 'CMA', 'Mom   ']],window=rwindow)

    rres = model.fit()
    params = rres.params

    price['y_hat'] = (params * price[['Mkt-RF','SMB','HML','RMW', 'CMA', 'Mom   ']]).sum(axis = 1)
    price['abnormal_ret'] = price['stock_ret-RF'] - price['y_hat']
    price = price.query('y_hat != 0') # filter out the first xxx rows


    # step 5: map out and save CAR
    print(price)
    price.to_parquet(addr + f'{companyid}.parquet')

    return 0



# internal import
from gff.gff_function import famaFrench5Factor, momentumFactor
from capitaliq.databaseManager import get_hist_miadj_pricing
from car.calc_et_car import calc_et_car

from car.calc_car import calculate_car

if __name__ == "__main__":

    universe = pd.read_csv('data/us_et_ref.csv', index_col = [0])# .query('companyid == 24937')

    # just to check whetehr our pricing function extract the right thing 
    # print(get_hist_miadj_pricing('2020-01-01', '2025-01-01', [24937, ])[['divadjclose', 'pricedate']])

    # this step calculate one day car for every company in the universe 
    for companyid in tqdm.tqdm(universe['companyid'].unique()):
        # print(companyid)
        if not os.path.exists(f'data/car_data/v1/{companyid}.parquet'):
            calculate_car(companyid, addr='data/car_data/v1/', start_date='2000-01-01', end_date='2030-06-01')
            print(f'{companyid} is done!')

    # this step calculate car but in aggregated manner for every company in the universe
    #   and output one file in total
    if False:
        calc_et_car(universe=universe)
    