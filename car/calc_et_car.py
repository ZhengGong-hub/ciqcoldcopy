# do this after calc_car.py
import tqdm
import pandas as pd 
import warnings
import sys
import os

ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

from fhandler.fileHandler import FileHandler
# internal
from capitaliq.databaseManager import get_hist_miadj_pricing

warnings.filterwarnings("ignore")

def calc_et_car(universe):

    res = []
    for _, row in tqdm.tqdm(universe.iterrows()):
        cid = row['companyid']
        print(row)
        assert False
        ec_et = pd.to_datetime(row['ec_et'])# .tz_localize(None)
        ec_et_day = pd.to_datetime(row['ec_et_day'])
        print(ec_et)
        # print(ec_et_day.year)

        car_data_addr = f'data/car_data/v1/{cid}.parquet'
        if FileHandler.check_file_existence(path='data/car_data/v1/', ext='.parquet', filename=cid):
            car = pd.read_parquet(car_data_addr)
        else:
            print(f'companyid {cid} does not exist!')
            continue 
        
        print(car)
        assert False

        # one_d_car, ten_d_car, one_m_car, one_q_car
        
        if ec_et.hour < 12:
            car = car.query('pricedate >= @ec_et_day')
        elif ec_et.hour >= 16:
            car = car.query('pricedate > @ec_et_day')
        else:
            print(f'no such market indicator type! {ec_et}')
            continue
        car.reset_index(inplace = True, drop = True)
        # print(car)

        car['compound'] = (1 + car['abnormal_ret']/100).cumprod()
        car.reset_index(inplace = True, drop = True)
        try:
            one_d_car = car['compound'].iloc[0] - 1
            one_w_car = car['compound'].iloc[5] - 1
            one_w_car_minus_one_d_car = car['compound'].iloc[5] / car['compound'].iloc[0] - 1
        except:
            one_d_car = None
            one_w_car = None
            one_w_car_minus_one_d_car = None
        try:
            one_m_car = car['compound'].iloc[22] - 1
            one_m_car_minus_one_w_car = car['compound'].iloc[22] / car['compound'].iloc[5] - 1
        except:
            one_m_car = None
            one_m_car_minus_one_w_car = None
        try:
            one_q_car = car['compound'].iloc[66] - 1
            one_q_car_minus_one_m_car = car['compound'].iloc[66] / car['compound'].iloc[22] - 1
        except:
            one_q_car = None
            one_q_car_minus_one_m_car = None
        res.append([row['transcriptid'], one_d_car, one_w_car, one_w_car_minus_one_d_car, one_m_car, one_m_car_minus_one_w_car , one_q_car, one_q_car_minus_one_m_car])

    df = pd.DataFrame(res, columns = ['transcriptid', 'one_d_car', 'one_w_car', 'one_w_car_minus_one_d_car', 'one_m_car', 'one_m_car_minus_one_w_car', 'one_q_car', 'one_q_car_minus_one_m_car'])
    print(df)
    os.makedirs('data/car_et_data', exist_ok=True)
    df.to_csv('data/car_et_data/car_et_total.csv')



if __name__ == "__main__":

    universe = pd.read_csv('/home/ubuntu/ciqcoldcopy/data/us_et_ref.csv', index_col = [0])
    calc_et_car(universe=universe)
    