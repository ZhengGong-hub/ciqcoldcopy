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

        ec_et = pd.to_datetime(row['ec_et'])# .tz_localize(None)
        ec_et_day = pd.to_datetime(ec_et.date())
        print(ec_et)
        # print(ec_et_day.year)

        car_data_addr = f'data/car_data/v2/{cid}.parquet'
        if FileHandler.check_file_existence(path='data/car_data/v2/', ext='.parquet', filename=cid):
            car = pd.read_parquet(car_data_addr)
        else:
            print(f'companyid {cid} does not exist!')
            continue 
        
        print(car)

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

        one_d_car = car['one_d_car'].iloc[0]
        one_w_car = car['one_w_car'].iloc[0]
        one_m_car = car['one_m_car'].iloc[0]
        one_q_car = car['one_q_car'].iloc[0]

        res.append([row['transcriptid'], one_d_car, one_w_car, one_m_car , one_q_car])
        print(res)
        assert False

    df = pd.DataFrame(res, columns = ['transcriptid', 'one_d_car', 'one_w_car', 'one_m_car', 'one_q_car'])
    print(df)
    os.makedirs('data/car_et_data', exist_ok=True)
    df.to_csv('data/car_et_data/car_et_total.csv')



if __name__ == "__main__":

    universe = pd.read_csv('/home/ubuntu/ciqcoldcopy/data/et_ref/complete_info/us_et_ref.csv')
    calc_et_car(universe=universe)
    