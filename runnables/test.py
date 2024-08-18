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

if __name__ == '__main__':
    main()