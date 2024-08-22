# need proper logging!

import pandas as pd 
import glob
import datetime
from datetime import date, timedelta
import logging
import os

import sys
ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 

sys.path.append(ROOTPATH)
from capitaliq.databaseManager import get_guidances, get_epsestimatediff_ref_co, earnings_given_keydevid, get_transcript, eventtoevent, get_transcript_ref_earliest_new, get_pit_universe_global, get_cur_mc_global, get_act_q_ref_co, earnings_on_the_date

et_ref = pd.read_csv('/home/ubuntu/ciqcoldcopy/data/us_et_ref.csv', index_col = [0])[['transcriptid', 'keydevid', 'companyid', 'earningscalldateutc', 'fiscalyear', 'fiscalquarter']]
et_ref['ec_et'] = pd.to_datetime(et_ref['earningscalldateutc']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['earningscalldateutc'], inplace = True)
et_ref.drop_duplicates(subset=['keydevid'], keep = False, inplace = True) # multitranscript version for the same transcripts # drop all duplicates totallY!
et_ref.drop_duplicates(subset=['fiscalyear', 'fiscalquarter', 'companyid'], keep = False, inplace = True)  # multitranscript version for the same transcripts # drop all duplicates totallY!
et_ref.dropna(subset=['fiscalyear'], inplace = True)
# print(et_ref)

# et_ref = et_ref.query('fiscalyear == @fy')

universe = pd.read_csv('/home/ubuntu/ciqcoldcopy/data/us_universe.csv')
# print(universe)

# merge on companyname  
# merge on marketcap 
et_ref = pd.merge(et_ref, universe[['companyname', 'companyid']], on = 'companyid', how = 'left')
print(et_ref)

# merge on earnings datetime 
earningsdate = earnings_on_the_date(et_ref['companyid'].values)
et_ref = pd.merge(et_ref, earningsdate[['companyid', 'earningsdate', 'marketindicatortypename', 'fiscalyear', 'fiscalquarter']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['earningsdate'] = pd.to_datetime(et_ref['earningsdate']) # .dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
# print(et_ref[et_ref.duplicated(subset=['keydevid'])])
print(len(et_ref))

# -------- EPS normalized estimates --------- # 
# EPS normalized
EPSnormalized = get_act_q_ref_co(et_ref['companyid'].values, [100179, ], '2000-01-01')
EPSnormalized.to_csv('data/EPSnormalized.csv')

et_ref = pd.merge(et_ref, EPSnormalized[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['EPSnormalized_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'EPS_normalized'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS_normalized', 'EPSnormalized_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# EPS normalized difference
EPSnormalizedDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100330, ], '2000-01-01')
EPSnormalizedDiff.to_csv('data/EPSnormalizedDiff.csv')

et_ref = pd.merge(et_ref, EPSnormalizedDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'EPS_normalizedDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS_normalizedDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('EPS normalized estimates FINISHED')

# -------- EPS estimates --------- # 
# EPS 
EPS = get_act_q_ref_co(et_ref['companyid'].values, [100284, ], '2000-01-01')
EPS.to_csv('data/EPS.csv')

et_ref = pd.merge(et_ref, EPS[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['EPS_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'EPS'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS', 'EPS_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# EPS difference
EPSDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100360, ], '2000-01-01')
EPSDiff.to_csv('data/EPSDiff.csv')

et_ref = pd.merge(et_ref, EPSDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'EPSDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPSDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('EPS estimates FINISHED')

# -------- revenue estimates --------- # 
# revenue 
revenue = get_act_q_ref_co(et_ref['companyid'].values, [100186, ], '2000-01-01')
revenue.to_csv('data/revenue.csv')

et_ref = pd.merge(et_ref, revenue[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['revenue_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'revenue'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'revenue', 'revenue_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# revenue difference
revenueDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100332, ], '2000-01-01')
revenueDiff.to_csv('data/revenueDiff.csv')

et_ref = pd.merge(et_ref, revenueDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'revenueDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'revenueDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('revenue estimates FINISHED')

os.makedirs('data/et_ref/complete_info', exist_ok=True)
et_ref.to_csv(f'data/et_ref/complete_info/us_et_ref.csv')



    # if False:
    #     # merge on earnings datetime 
    #     # estimate median EPS 
    #     earnings_estimate_median = get_act_q_ref_co(et_ref['companyid'].values, [100174, ], '2023-06-01')
    #     # print(earnings.columns)
    #     et_ref = pd.merge(et_ref, earnings_estimate_median[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left').dropna(subset=['MC_bUSD'])
    #     et_ref.rename(columns = {'dataitemvalue': 'estimate_EPS_normalized'}, inplace = True)
    #     et_ref.sort_values(by = ['effectivedate'], inplace = True)
    #     et_ref.drop_duplicates(subset=['companyid', 'fiscalyear', 'fiscalquarter'], keep = 'last', inplace = True)
    #     et_ref.drop(columns = ['effectivedate'], inplace = True)
    #     print(et_ref)
    #     # et_ref.to_csv('temp.csv')

    #     def timing(x):
    #         if x.hour <= 12:
    #             return 'before'
    #         elif x.hour >= 16:
    #             return 'after'
    #         else:
    #             return 'unknown'

    #     # get_price
    #     # close - close 
    #     et_ref['e_status'] = et_ref['e_et'].apply(timing)
    #     et_ref['ec_status'] = et_ref['ec_et'].apply(timing)
    #     et_ref['diff'] = (et_ref['ec_et'] - et_ref['e_et'])/pd.Timedelta(hours=1)

    #     print(et_ref)
    #     et_ref.to_csv('temp.csv')



    # # archive

    # # ete = eventtoevent(keydevids = et_ref['keydevid'].values)
    # # et_ref = pd.merge(et_ref, ete, on = ['keydevid'], how = 'left').drop_duplicates(subset=['keydevid', 'linkedkeydevid']).dropna(subset = ['linkedkeydevid'])
    # # earnings = earnings_given_keydevid(ls_ids = et_ref['linkedkeydevid'].values)
    # # print(earnings)
    # # et_ref = pd.merge(et_ref, earnings, left_on = 'linkedkeydevid', right_on= 'linkedkeydevid', how = 'left').drop_duplicates(subset=['keydevid', 'linkedkeydevid'])
    # # print(et_ref)
    # # et_ref.to_csv('ssss.csv')

    # # et_ref['diff'] = (et_ref['ec_et'] - et_ref['earningsdate'])/pd.Timedelta(hours=1)

    # # # EPS normalized Guidance
    # # EPSnormalizedGuide = get_guidances(et_ref['companyid'].values, [22383, ], '2023-06-01')
    # # EPSnormalizedGuide.to_csv('EPSnormalizedGuide.csv')

    # # et_ref = pd.merge(et_ref, EPSnormalizedGuide[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate', 'todate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
    # # et_ref.rename(columns = {'dataitemvalue': 'EPSnormalizedGuideHigh'}, inplace = True)
    # # et_ref.to_csv('temp.csv')
    # # print(len(et_ref))