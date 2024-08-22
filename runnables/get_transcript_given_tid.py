# need proper logging!

import pandas as pd 
import glob
import datetime
from datetime import date, timedelta
import logging
import tqdm
import os

import sys
ROOTPATH = '/home/ubuntu/ciqcoldcopy/' # for importing and reference management 
sys.path.append(ROOTPATH)

# internal
from capitaliq.databaseManager import get_all_transcript, get_all_us_universe, get_transcript


print('running get_et.py')
print(date.today().strftime("%Y-%m-%d"))

# metadata
transcripts_ref = pd.read_csv('data/us_et_ref.csv')
print('total: ' + str(len(transcripts_ref['transcriptid'].unique())))

total_ecs = transcripts_ref['transcriptid'].unique()

transcripts = get_transcript(total_ecs)
transcripts.to_parquet('data/et/all_transcripts.parquet')
for transcriptid in tqdm.tqdm(transcripts['transcriptid'].unique()):
    os.makedirs('data/et/individual', exist_ok=True)
    transcripts.query("transcriptid == @transcriptid").to_parquet(f"data/et/individual/{transcriptid}.parquet")

