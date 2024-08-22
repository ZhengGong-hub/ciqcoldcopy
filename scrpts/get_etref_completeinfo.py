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

# hyper parameters
EPSnormalized_PATH = 'data/EPSnormalized.csv'
EPSnormalizedDiff_PATH = 'data/EPSnormalizedDiff.csv'
EPS_PATH = 'data/EPS.csv'
EPSDiff_PATH = 'data/EPSDiff.csv'
revenue_PATH = 'data/revenue.csv'
revenueDiff_PATH = 'data/revenueDiff.csv'


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
if os.path.exists(EPSnormalized_PATH):
    EPSnormalized = pd.read_csv(EPSnormalized_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(EPSnormalized_PATH), exist_ok=True)
    EPSnormalized = get_act_q_ref_co(et_ref['companyid'].values, [100179, ], '2000-01-01')
    EPSnormalized.to_csv(EPSnormalized_PATH)

et_ref = pd.merge(et_ref, EPSnormalized[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['EPSnormalized_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'EPS_normalized'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS_normalized', 'EPSnormalized_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# EPS normalized difference
if os.path.exists(EPSnormalizedDiff_PATH):
    EPSnormalizedDiff = pd.read_csv(EPSnormalizedDiff_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(EPSnormalizedDiff_PATH), exist_ok=True)
    EPSnormalizedDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100330, ], '2000-01-01')
    EPSnormalizedDiff.to_csv(EPSnormalizedDiff_PATH)

et_ref = pd.merge(et_ref, EPSnormalizedDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'EPS_normalizedDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS_normalizedDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('EPS normalized estimates FINISHED')

# -------- EPS estimates --------- # 
# EPS 
if os.path.exists(EPS_PATH):
    EPS = pd.read_csv(EPS_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(EPS_PATH), exist_ok=True)
    EPS = get_act_q_ref_co(et_ref['companyid'].values, [100284, ], '2000-01-01')
    EPS.to_csv(EPS_PATH)

et_ref = pd.merge(et_ref, EPS[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['EPS_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'EPS'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPS', 'EPS_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# EPS difference
if os.path.exists(EPSDiff_PATH):
    EPSDiff = pd.read_csv(EPSDiff_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(EPSDiff_PATH), exist_ok=True)
    EPSDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100360, ], '2000-01-01')
    EPSDiff.to_csv(EPSDiff_PATH)

et_ref = pd.merge(et_ref, EPSDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'EPSDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'EPSDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('EPS estimates FINISHED')

# -------- revenue estimates --------- # 
# revenue 
if os.path.exists(revenue_PATH):
    revenue = pd.read_csv(revenue_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(revenue_PATH), exist_ok=True)
    revenue = get_act_q_ref_co(et_ref['companyid'].values, [100186, ], '2000-01-01')
    revenue.to_csv(revenue_PATH)

et_ref = pd.merge(et_ref, revenue[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'effectivedate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref['revenue_et'] = pd.to_datetime(et_ref['effectivedate']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
et_ref.drop(columns = ['effectivedate'], inplace = True)
et_ref.rename(columns = {'dataitemvalue': 'revenue'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'revenue', 'revenue_et'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
print(len(et_ref))

# revenue difference
if os.path.exists(revenueDiff_PATH):
    revenueDiff = pd.read_csv(revenueDiff_PATH, index_col = [0])
else:
    os.makedirs(os.path.dirname(revenueDiff_PATH), exist_ok=True)
    revenueDiff = get_epsestimatediff_ref_co(et_ref['companyid'].values, [100332, ], '2000-01-01')
    revenueDiff.to_csv(revenueDiff_PATH)

et_ref = pd.merge(et_ref, revenueDiff[['companyid', 'fiscalyear', 'fiscalquarter', 'dataitemvalue', 'asofdate']], on = ['companyid', 'fiscalyear', 'fiscalquarter'], how = 'left')
et_ref.rename(columns = {'dataitemvalue': 'revenueDiff'}, inplace = True)
et_ref.drop_duplicates(subset=['keydevid', 'revenueDiff', 'asofdate'], keep = 'first', inplace = True) # if this three columns are all the same we can just keep them, just keep one
et_ref.drop(columns = ['asofdate'], inplace = True)
print(len(et_ref))
print('revenue estimates FINISHED')

os.makedirs('data/et_ref/complete_info', exist_ok=True)
et_ref.to_csv(f'data/et_ref/complete_info/us_et_ref.csv')
print(et_ref)