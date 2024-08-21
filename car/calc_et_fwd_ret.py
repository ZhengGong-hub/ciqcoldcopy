# do this after calc_car.py
import tqdm
import pandas as pd 
import warnings
import sys
from statsmodels.regression.rolling import RollingOLS

ROOTPATH = '/Users/zhenggong/Documents/Github/ba_thesis/' # for importing and reference management 
sys.path.append(ROOTPATH)

from fhandler.fileHandler import FileHandler
# internal
from capitaliq.databaseManager import get_hist_miadj_pricing

warnings.filterwarnings("ignore")

def calc_et_car(universe):

    res = []
    for _, row in tqdm.tqdm(universe.iterrows()):
        cid = row['companyid']
        ec_et = pd.to_datetime(row['ec_et'])# .tz_localize(None)
        ec_et_day = pd.to_datetime(row['ec_et_day'])
        print(ec_et)
        # print(ec_et_day.year)

        # we do not want to calculate car for the 2023 earnings call
        if ec_et_day.year >= 2023:
            continue

        car_data_addr = f'data/fwd_ret_data/v1/{cid}.parquet'
        if FileHandler.check_file_existence(path='data/car_data/v1/', ext='.parquet', filename=cid):
            fwd_ret = pd.read_parquet(car_data_addr)
        else:
            print(f'companyid {cid} does not exist!')
            continue 

        # print(fwd_ret)

        # step 4: run linear regression
        fwd_ret_hist = fwd_ret.query("pricedate < @ec_et_day")
        fwd_ret_hist['intercept'] = 1
        model = RollingOLS(endog =fwd_ret_hist['stock_ret_close'].values , exog=fwd_ret_hist[['sp500_ret_close' , 'intercept']],window=252)

        try:
            rres = model.fit()
            beta = float(rres.params.iloc[-2])
        except:
            beta = 1
        
        if ec_et.hour < 9:
            fwd_ret = fwd_ret.query('pricedate >= @ec_et_day')
            fwd_ret['stock_ret_compound'] = (1 + fwd_ret['stock_ret_open'].astype(float)).cumprod()
            fwd_ret['sp500_ret_compound'] = (1 + fwd_ret['sp500_ret_open'].astype(float)).cumprod()


        elif ec_et.hour >= 16:
            fwd_ret = fwd_ret.query('pricedate > @ec_et_day')
            fwd_ret['stock_ret_compound'] = (1 + fwd_ret['stock_ret_open'].astype(float)).cumprod()
            fwd_ret['sp500_ret_compound'] = (1 + fwd_ret['sp500_ret_open'].astype(float)).cumprod()

        elif ec_et.hour >= 10 and ec_et.hour < 15:
            fwd_ret = fwd_ret.query('pricedate >= @ec_et_day')
            fwd_ret['stock_ret_compound'] = (1 + fwd_ret['stock_ret_close'].astype(float)).cumprod()
            fwd_ret['sp500_ret_compound'] = (1 + fwd_ret['sp500_ret_close'].astype(float)).cumprod()

        else:
            print(f'no such market indicator type! {ec_et}')
            continue
        
        try:
            _1d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[0] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[0] - 1) * beta)
        except:
            _1d_fwd_ret = None

        try:
            _2d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[1] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[1] - 1) * beta)
        except:
            _2d_fwd_ret = None

        try:
            _3d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[2] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[2] - 1) * beta)
        except:
            _3d_fwd_ret = None
        
        try:
            _4d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[3] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[3] - 1) * beta)
        except:
            _4d_fwd_ret = None

        try:
            _5d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[4] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[4] - 1) * beta)
        except:
            _5d_fwd_ret = None

        try:
            _10d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[9] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[9] - 1) * beta)
        except:
            _10d_fwd_ret = None
        
        try:
            _22d_fwd_ret = fwd_ret['stock_ret_compound'].iloc[21] - 1  - ((fwd_ret['sp500_ret_compound'].iloc[21] - 1) * beta)
        except:
            _22d_fwd_ret = None

        res.append([row['transcriptid'], _1d_fwd_ret, _2d_fwd_ret, _3d_fwd_ret, _4d_fwd_ret, _5d_fwd_ret , _10d_fwd_ret, _22d_fwd_ret])

    df = pd.DataFrame(res, columns = ['transcriptid', '_1d_fwd_ret', '_2d_fwd_ret', '_3d_fwd_ret', '_4d_fwd_ret', '_5d_fwd_ret', '_10d_fwd_ret', '_22d_fwd_ret'])
    print(df)
    df.to_csv('data/fwd_et_data/fwd_et_total.csv')



if __name__ == "__main__":

    universe = pd.read_csv('data/processed/universeAugmented.csv', index_col = [0])
    calc_et_car(universe=universe)
    