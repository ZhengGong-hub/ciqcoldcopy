import pandas as pd 
import os 

def get_monthly_return(price:pd.DataFrame, outpupt_path:'data/price/monthly_return.csv') -> pd.DataFrame:

    if os.path.exists(outpupt_path):
        monthly = pd.read_csv(outpupt_path, index_col = [0])
        print('monthly return info FINISHED')
        return monthly

    monthly = price[['companyid', 'pricedate', 'divadjclose']].groupby(['companyid', pd.Grouper(key='pricedate', freq='M')]).last().reset_index()
    monthly["Return"] = monthly.groupby("companyid")["divadjclose"].pct_change(1)
    monthly["vol"] = monthly.groupby("companyid")["Return"].rolling(48, min_periods=12).std().reset_index(0, drop=True)
    monthly.to_csv(outpupt_path)
    print('monthly return info FINISHED')
    return monthly