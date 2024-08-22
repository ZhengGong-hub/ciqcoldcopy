import pandas as pd 
import sys 
import os
import tqdm
import math
import numpy as np

ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal
from capitaliq.databaseManager import search_fundamental, get_historical_fundamental, get_cur_mc_global, get_hist_earnings_release_dates, get_hist_miadj_pricing
from car.calc_et_car import calc_et_car
from src.merge_marketcap import merge_marketcap
from src.merge_tot_equity import merge_tot_equity
from src.get_earning_release_date import get_earning_release_date
from src.earnings_change import merge_earnings_estimates
from src.monthly_return import get_monthly_return

from car.calc_car import calculate_car

# hyper parameters
EPSnormalized_PATH = 'data/EPSnormalized.csv'
EPSnormalizedDiff_PATH = 'data/EPSnormalizedDiff.csv'
EPS_PATH = 'data/EPS.csv'
EPSDiff_PATH = 'data/EPSDiff.csv'
revenue_PATH = 'data/revenue.csv'
revenueDiff_PATH = 'data/revenueDiff.csv'
marketcap_PATH = 'data/mc/marketcap.csv'
tot_equity_PATH = 'data/fundamentals/tot_equity.csv'
price_PATH = 'data/price/us_price.csv'
earnings_release_date_PATH = 'data/earning_release_date.csv'
monthly_return_PATH = 'data/price/monthly_return.csv'

if __name__ == "__main__":
    # Load the universe, this is starting point
    #     should later filter out based on which earnings trasncript we have
    universe = pd.read_csv('/home/ubuntu/ciqcoldcopy/data/us_et_ref.csv', index_col = [0])

    universe['ec_et'] = pd.to_datetime(universe['earningscalldateutc']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    # get rid of the timezone 
    universe['ec_et'] = universe['ec_et'].dt.tz_localize(None)
    universe['ec_et_day'] = pd.to_datetime(universe['ec_et'].dt.strftime('%Y-%m-%d'))
    universe.drop_duplicates(subset=['keydevid'], keep = False, inplace = True) # multitranscript version for the same transcripts # drop all duplicates totallY!
    universe.drop_duplicates(subset=['fiscalyear', 'fiscalquarter', 'companyid'], keep = False, inplace = True)  # multitranscript version for the same transcripts # drop all duplicates totallY!
    universe.dropna(subset=['fiscalyear'], inplace = True)
    print(universe)

    # feature engineering
    # log market cap
    universe = merge_marketcap(universe, marketcap_PATH)
    universe = merge_tot_equity(universe, tot_equity_PATH)
    universe['btm'] = universe['tot_equity'] / universe['marketcap'] # book to market ratio 

    # earnings release date 
    earning_release_date = get_earning_release_date(universe, earnings_release_date_PATH)

    universe = pd.merge(universe, earning_release_date[['companyid', 'fiscalyear', 'fiscalquarter', 'earningsdate', 'last_earningsdate', 'earningsdate-5days', 'last_earnings+5days']], on=['companyid', 'fiscalyear', 'fiscalquarter'], how='left')
    universe.dropna(subset=['earningsdate-5days'], inplace=True)

    # price
    price = pd.read_csv(price_PATH, index_col=[0])
    price['pricedate'] = pd.to_datetime(price['pricedate'])
    # print(price.head())

    universe.sort_values(by=['earningsdate-5days'], inplace=True)
    universe = pd.merge_asof(universe, price[['companyid', 'pricedate', 'divadjclose']].rename(columns={"pricedate":"earningsdate-5days", "divadjclose":"earningsdate-5days_priceclose"}), on=['earningsdate-5days'], by = 'companyid', direction = 'backward', tolerance=pd.Timedelta('5 days'))

    universe.sort_values(by=['last_earnings+5days'], inplace=True)    
    universe = pd.merge_asof(universe, price[['companyid', 'pricedate', 'divadjclose']].rename(columns={"pricedate":"last_earnings+5days", "divadjclose":"last_earnings+5days_priceclose"}), on=['last_earnings+5days'], by = 'companyid', direction = 'backward', tolerance=pd.Timedelta('5 days'))
    
    universe['past_ret'] = (universe['earningsdate-5days_priceclose'] - universe['last_earnings+5days_priceclose']) / universe['last_earnings+5days_priceclose']
    
    ## filter out the earnings release and earnings date are too far apart
    universe['hours_ec_earning_delay'] = (universe['ec_et'] - universe['earningsdate']).dt.total_seconds() / 3600 # in hours
    universe = universe.query('hours_ec_earning_delay <= 24 and hours_ec_earning_delay >= -12')

    monthly_ret = get_monthly_return(price, monthly_return_PATH)
    monthly_ret['pricedate'] = pd.to_datetime(monthly_ret['pricedate'])
    monthly_ret.sort_values(by=['pricedate'], inplace=True)
    universe.sort_values(by=['earningsdate'], inplace=True)
    universe = pd.merge_asof(universe, monthly_ret[['companyid', 'pricedate', 'vol']].rename(columns={"pricedate":"earningsdate"}), on=['earningsdate'], by = 'companyid', direction = 'backward', tolerance=pd.Timedelta('180 days'))

    # get earnings change related features
    universe = merge_earnings_estimates(
        universe=universe, 
        eps_path=EPS_PATH, 
        eps_norm_path=EPSnormalized_PATH, 
        eps_norm_diff_path=EPSnormalizedDiff_PATH, 
        rev_diff_path=revenueDiff_PATH, 
        price=price, 
        mc_PATH=marketcap_PATH)
    

    # -------- EPS normalized estimates --------- # 
    # EPS normalized
    if os.path.exists(EPSnormalized_PATH):
        EPSnormalized = pd.read_csv(EPSnormalized_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(EPSnormalized_PATH), exist_ok=True)
        EPSnormalized = get_act_q_ref_co(universe['companyid'].values, [100179, ], '2000-01-01')
        EPSnormalized.to_csv(EPSnormalized_PATH)

    universe = pd.merge(universe, EPSnormalized[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe['EPSnormalized_et'] = pd.to_datetime(universe['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    universe.drop(columns = ['effectivedate'], inplace = True)
    universe.rename(columns = {'dataitemvalue': 'EPS_normalized'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'EPS_normalized', 'EPSnormalized_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    print(len(universe))

    # EPS normalized difference
    if os.path.exists(EPSnormalizedDiff_PATH):
        EPSnormalizedDiff = pd.read_csv(EPSnormalizedDiff_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(EPSnormalizedDiff_PATH), exist_ok=True)
        EPSnormalizedDiff = get_epsestimatediff_ref_co(universe['companyid'].values, [100330, ], '2000-01-01')
        EPSnormalizedDiff.to_csv(EPSnormalizedDiff_PATH)

    universe = pd.merge(universe, EPSnormalizedDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe.rename(columns = {'dataitemvalue': 'EPS_normalizedDiff'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'EPS_normalizedDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    universe.drop(columns = ['asofdate'], inplace = True)
    print(len(universe))
    print('EPS normalized estimates FINISHED')

    # -------- EPS estimates --------- # 
    # EPS 
    if os.path.exists(EPS_PATH):
        EPS = pd.read_csv(EPS_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(EPS_PATH), exist_ok=True)
        EPS = get_act_q_ref_co(universe['companyid'].values, [100284, ], '2000-01-01')
        EPS.to_csv(EPS_PATH)

    universe = pd.merge(universe, EPS[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe['EPS_et'] = pd.to_datetime(universe['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    universe.drop(columns = ['effectivedate'], inplace = True)
    universe.rename(columns = {'dataitemvalue': 'EPS'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'EPS', 'EPS_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    print(len(universe))

    # EPS difference
    if os.path.exists(EPSDiff_PATH):
        EPSDiff = pd.read_csv(EPSDiff_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(EPSDiff_PATH), exist_ok=True)
        EPSDiff = get_epsestimatediff_ref_co(universe['companyid'].values, [100360, ], '2000-01-01')
        EPSDiff.to_csv(EPSDiff_PATH)

    universe = pd.merge(universe, EPSDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe.rename(columns = {'dataitemvalue': 'EPSDiff'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'EPSDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    universe.drop(columns = ['asofdate'], inplace = True)
    print(len(universe))
    print('EPS estimates FINISHED')

    # -------- revenue estimates --------- # 
    # revenue 
    if os.path.exists(revenue_PATH):
        revenue = pd.read_csv(revenue_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(revenue_PATH), exist_ok=True)
        revenue = get_act_q_ref_co(universe['companyid'].values, [100186, ], '2000-01-01')
        revenue.to_csv(revenue_PATH)

    universe = pd.merge(universe, revenue[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe['revenue_et'] = pd.to_datetime(universe['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    universe.drop(columns = ['effectivedate'], inplace = True)
    universe.rename(columns = {'dataitemvalue': 'revenue'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'revenue', 'revenue_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    print(len(universe))

    # revenue difference
    if os.path.exists(revenueDiff_PATH):
        revenueDiff = pd.read_csv(revenueDiff_PATH, index_col = [0])
    else:
        os.makedirs(os.path.dirname(revenueDiff_PATH), exist_ok=True)
        revenueDiff = get_epsestimatediff_ref_co(universe['companyid'].values, [100332, ], '2000-01-01')
        revenueDiff.to_csv(revenueDiff_PATH)

    universe = pd.merge(universe, revenueDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    universe.rename(columns = {'dataitemvalue': 'revenueDiff'}, inplace = True)
    universe.drop_duplicates(subset=['keydevid', 'revenueDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
    universe.drop(columns = ['asofdate'], inplace = True)
    print(len(universe))
    print('revenue estimates FINISHED')


    # merge car
    car = pd.read_csv('data/car_et_data/car_et_total.csv', index_col=[0])
    universe = pd.merge(universe, car, on='transcriptid', how='left')

    # final re-clean
    universe.drop_duplicates(subset=['companyid', 'fiscalyear', 'fiscalquarter'], keep='first', inplace=True)
    # -------- save --------- #
    os.makedirs('data/et_ref/complete_info', exist_ok=True)
    universe.to_csv(f'data/et_ref/complete_info/us_et_ref.csv')
    print(universe)
