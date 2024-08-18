import pandas as pd 
import sys 
import os
import tqdm
import math
import numpy as np

ROOTPATH = '/Users/zhenggong/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal
from capitaliq.databaseManager import get_all_transcript, get_all_us_universe

# main
def main():
    df = get_all_us_universe()
    print(df)

    # save 
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/us_universe.csv', index=False)


    et_ref = get_all_transcript(df['company_id'].unique())
    et_ref.to_csv(f'data/us_et_ref.csv')

if __name__ == '__main__':
    main()