import pandas as pd 

def merge_earnings_estimates(
    eps_path='data/processed/revenue.csv', 
    eps_norm_path='data/processed/EPSnormalized.csv', 
    eps_norm_diff_path = 'data/processed/EPSnormalizedDiff.csv',
    rev_diff_path = 'data/processed/revenueDiff.csv',
    price: pd.DataFrame=None, 
    mc_PATH: pd.DataFrame=None,
    universe: pd.DataFrame=None) -> pd.DataFrame:

    ## ----- ##
    # calculate rev change
    eps = pd.read_csv(eps_path, index_col=[0]) # can also use EPS.csv
    eps.sort_values(by=['companyid', 'fiscalyear', 'fiscalquarter', 'tradingitemid', 'dataitemvalue'], inplace=True)
    eps.drop_duplicates(subset=['companyid', 'fiscalyear', 'fiscalquarter'], keep='first', inplace=True)
    # print(eps) # very little lost or confusing data

    eps['yoy_last_year'] = eps['fiscalyear'] - 1
    eps = pd.merge(eps, eps[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue']].rename(columns={'dataitemvalue': 'lastvalue', 'fiscalyear': 'yoy_last_year'}), left_on = ['companyid', 'yoy_last_year', 'fiscalquarter'], right_on=['companyid', 'yoy_last_year', 'fiscalquarter'], how = 'left')
    eps['eps_change'] = eps['dataitemvalue'] - eps['lastvalue']
    eps["eps_change_vol"] = eps.groupby("companyid")["eps_change"].rolling(20, min_periods=10).std().reset_index(0, drop=True)
    eps['standardized_rev_change'] = eps['eps_change'] / eps['eps_change_vol']
    # print(eps)

    ## ----- ##
    # calculate eps norm change
    eps_norm = pd.read_csv(eps_norm_path, index_col=[0]) # can also use EPS.csv
    eps_norm.sort_values(by=['companyid', 'fiscalyear', 'fiscalquarter', 'tradingitemid', 'dataitemvalue'], inplace=True)
    eps_norm.drop_duplicates(subset=['companyid', 'fiscalyear', 'fiscalquarter'], keep='first', inplace=True)
    # print(eps) # very little lost or confusing data

    eps_norm['yoy_last_year'] = eps_norm['fiscalyear'] - 1
    eps_norm = pd.merge(eps_norm, eps_norm[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue']].rename(columns={'dataitemvalue': 'lastvalue', 'fiscalyear': 'yoy_last_year'}), left_on = ['companyid', 'yoy_last_year', 'fiscalquarter'], right_on=['companyid', 'yoy_last_year', 'fiscalquarter'], how = 'left')
    eps_norm['eps_change'] = eps_norm['dataitemvalue'] - eps_norm['lastvalue']
    eps_norm["eps_change_vol"] = eps_norm.groupby("companyid")["eps_change"].rolling(20, min_periods=10).std().reset_index(0, drop=True)
    eps_norm['standardized_eps_norm_change'] = eps_norm['eps_change'] / eps_norm['eps_change_vol']
    # print(eps_norm)

    # calculate earnings surprise
    price['pricedate+5'] = pd.to_datetime(price['pricedate']) + pd.Timedelta(days=5) # shift data five day later
    price.dropna(subset=['divadjclose'], inplace=True)

    epsNormDiff = pd.read_csv(eps_norm_diff_path, index_col=[0])
    epsNormDiff['asofdate'] = pd.to_datetime(epsNormDiff['asofdate'])
    epsNormDiff.sort_values(by=['asofdate'], inplace=True)
    epsNormDiff = pd.merge_asof(epsNormDiff, price[['companyid', 'pricedate+5', 'divadjclose']], left_on = ['asofdate'], right_on=['pricedate+5'], tolerance=pd.Timedelta("10d"), by=['companyid'], direction = 'backward')
    epsNormDiff['EPS_norm_earnSurprise'] = 4 * epsNormDiff['dataitemvalue'] / epsNormDiff['divadjclose']
    # print(epsNormDiff)

    # calculate market cap
    mc = pd.read_csv(mc_PATH, index_col=[0])
    mc['pricedate+5'] = pd.to_datetime(mc['pricingdate']) + pd.Timedelta(days=5) # shift data five day later
    mc.sort_values(by=['pricedate+5'], inplace=True)

    epsDiff = pd.read_csv(rev_diff_path, index_col=[0])
    epsDiff['asofdate'] = pd.to_datetime(epsDiff['asofdate'])
    epsDiff.sort_values(by=['asofdate'], inplace=True)
    epsDiff = pd.merge_asof(epsDiff, mc[['companyid', 'pricedate+5', 'marketcap']], left_on = ['asofdate'], right_on=['pricedate+5'], tolerance=pd.Timedelta("10d"), by=['companyid'], direction = 'backward')
    epsDiff['rev_earnSurprise'] = 4 * epsDiff['dataitemvalue'] / epsDiff['marketcap']
    # print(epsDiff)

    universe = pd.merge(universe, eps[['companyid', 'fiscalyear', 'fiscalquarter', 'standardized_rev_change']], on=['companyid', 'fiscalyear', 'fiscalquarter'], how='left')
    universe = pd.merge(universe, eps_norm[['companyid', 'fiscalyear', 'fiscalquarter', 'standardized_eps_norm_change']], on=['companyid', 'fiscalyear', 'fiscalquarter'], how='left')

    universe = pd.merge(universe, epsDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'rev_earnSurprise']], on=['companyid', 'fiscalyear', 'fiscalquarter'], how='left')
    universe = pd.merge(universe, epsNormDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'EPS_norm_earnSurprise']], on=['companyid', 'fiscalyear', 'fiscalquarter'], how='left')

    print("earnings change related stuff done!")
    return universe