# getFamaFrenchFactors.py
# Author: Vash
# Version 0.0.4
# Last updated: 18 May 2019
# https://github.com/vashOnGitHub/getFamaFrenchFactors/blob/master/src/getFamaFrenchFactors.py

# forked by GZ
#   i deleted a lot of not useful functions.
# 
"""
This programme gets cleaned versions of factors including:
    * Fama French 3 factor (MRP, SMB, HML)
    * Momentum (MOM)
    * Carhart 4 factors (MRP, SMB, HML, MOM)
    * Fama French 5 factors (MRP, SMB, HML, RMW, CMA)

Updates in Version 0.0.4:
Replaces manual URL with scraped URL for initial futureproofing.

Updates in Version 0.0.3:
Adds support for annual data in addition to monthly data.
"""

import pandas as pd
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
import os

# Extract URLs to download
url = "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')

text_to_search = ['Fama/French 3 Factors', 'Momentum Factor (Mom)']
all_factors_text = soup.findAll('b', text=text_to_search)

home_url = "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
all_factor_links = []
for text in all_factors_text:
    links_for_factor = []  # Stores all links for a factor
    for sib in text.next_siblings:  # Find next element
        # URLs are stored in bold tags, hence...
        if sib.name == 'b':
            bold_tags = sib
            try:
                link = bold_tags.find('a')['href']
                links_for_factor.append(link)
            except TypeError:
                pass
    csv_links = [home_url + link for link in links_for_factor if 'csv' in link.lower()]
    txt_links = [home_url + link for link in links_for_factor if 'txt' in link.lower()]
    factor_dict = {'factor' : text, 'csv_links' : csv_links, 'txt_links' : txt_links}
    all_factor_links.append(factor_dict)


ff3factor_dict = dict(all_factor_links[0])
momAndOthers_dict = dict(all_factor_links[1])


def famaFrench5Factor(frequency='m'):
    '''
    Returns Fama French 5 factors (Market Risk Premium, SMB, HML, RMW, CMA),
    and the risk-free rate (RF)
    '''
    if frequency == 'd':
        #############
        # new  by GZ# 
        # to accelerate batch compute
        if os.path.exists('data/ff5_factors.csv'):
            ff5_factors = pd.read_csv('data/ff5_factors.csv')
        else:
            ff5_raw_data = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"

            ff5_factors = pd.read_csv(ff5_raw_data, skiprows=3)
            ff5_factors.to_csv('data/ff5_factors.csv', index=False)

        ff5_factors.rename(columns = {ff5_factors.columns[0] : 'date_ff_factors'},
                        inplace=True)

        # Convert dates to datetime objects (note: values will be int64)
        ff5_factors['date_ff_factors'] = pd.to_datetime(ff5_factors['date_ff_factors'],
                                                        format='%Y%m%d')
        # print(ff5_factors.tail(10))
        #############
    else:
        rows_to_skip = 3
        ff5_raw_data = ff3factor_dict['csv_links'][3]

        ff5_factors = pd.read_csv(ff5_raw_data, skiprows=rows_to_skip)

        ff5_factors.rename(columns = {ff5_factors.columns[0] : 'date_ff_factors'},
                        inplace=True)

        # Get index of annual factor returns
        annual_factor_index_loc = ff5_factors[
                ff5_factors.values == ' Annual Factors: January-December '].index


        # Clean annual and monthly versions
        if frequency == 'm':
            ff5_factors.drop(ff5_factors.index[annual_factor_index_loc[0]:], inplace=True)

            # Convert dates to pd datetime objects
            ff5_factors['date_ff_factors'] = pd.to_datetime(ff5_factors['date_ff_factors'],
                                                            format='%Y%m')
            # Shift dates to end of month
            ff5_factors['date_ff_factors'] = ff5_factors['date_ff_factors'].apply(
                lambda date : date + relativedelta(day = 1, months = +1, days = -1))

        elif frequency == 'a':
            # Extract annual data only
            ff5_factors.drop(ff5_factors.index[:annual_factor_index_loc[0]],
                            inplace=True)

            # Ignore copyright footer & first 2 header rows
            ff5_factors = ff5_factors.iloc[2:]
            ff5_factors.reset_index(inplace=True)
            ff5_factors.drop(columns=ff5_factors.columns[0], inplace=True)

            # Deal with spacing issues (e.g. '  1927' instead of '1927')
            ff5_factors['date_ff_factors'] = ff5_factors['date_ff_factors'].apply(
                lambda x : x.strip())

            # Convert dates to datetime objects (note: values will be int64)
            ff5_factors['date_ff_factors'] = pd.to_datetime(ff5_factors['date_ff_factors'],
                                                            format='%Y').dt.year.values

        # Convert all factors to numeric and decimals (%)
        for col in ff5_factors.columns[1:]:
            ff5_factors[col] = pd.to_numeric(ff5_factors[col]) / 100

    return ff5_factors

def momentumFactor(frequency='m'):
    '''
    Returns the Momentum factor

    Set frequency as:
        'm' for monthly factors
        'a' for annual factors

    '''
    if frequency == 'd':
        #############
        # new  by GZ# 
        # to accelerate batch compute
        if os.path.exists('data/factor_momentum.csv'):
            mom_factor = pd.read_csv('data/factor_momentum.csv')
        else:
            mom_raw_data = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"

            mom_factor = pd.read_csv(mom_raw_data, skiprows=13)
            mom_factor.to_csv('data/factor_momentum.csv', index=False)


        mom_factor.rename(columns = {mom_factor.columns[0] : 'date_ff_factors'},
                        inplace=True)

        mom_factor.drop(mom_factor.tail(1).index,inplace=True) # drop last n rows

        # print(mom_factor)

        # Convert dates to datetime objects (note: values will be int64)
        mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'],
                                                        format='%Y%m%d')
        # print(mom_factor.tail(10))
        #############
    else:
        rows_to_skip = 13
        mom_raw_data = momAndOthers_dict['csv_links'][0]

        mom_factor = pd.read_csv(mom_raw_data, skiprows=rows_to_skip)
        mom_factor.rename(columns = {mom_factor.columns[0] : 'date_ff_factors'},
                        inplace=True)

        # Get index of annual factor returns
        annual_factor_index_loc = mom_factor[
                mom_factor.values == 'Annual Factors:'].index
        # Clean annual and monthly versions
        if frequency == 'm':
            # Exclude annual factor returns
            mom_factor.drop(mom_factor.index[annual_factor_index_loc[0]:], inplace=True)

            # Convert dates to pd datetime objects
            mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'],
                                                        format='%Y%m')

            # Shift dates to end of month
            mom_factor['date_ff_factors'] = mom_factor['date_ff_factors'].apply(
                lambda date : date + relativedelta(day = 1, months = +1, days = -1))

        elif frequency == 'a':
            # Extract annual data only
            mom_factor.drop(mom_factor.index[:annual_factor_index_loc[0]],
                            inplace=True)

            # Ignore copyright footer & first 2 header rows
            mom_factor = mom_factor.iloc[3:-1]
            mom_factor.reset_index(inplace=True)
            mom_factor.drop(columns=mom_factor.columns[0], inplace=True)

            # Deal with spacing issues (e.g. '  1927' instead of '1927')
            mom_factor['date_ff_factors'] = mom_factor['date_ff_factors'].apply(
                lambda x : x.strip())

            # Convert dates to datetime objects (note: values will be int64)
            mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'],
                                                        format='%Y').dt.year.values

        # Convert all factors to numeric and decimals (%)
        for col in mom_factor.columns[1:]:
            mom_factor[col] = pd.to_numeric(mom_factor[col]) / 100

        # Rename momentum factor to eliminate white space
        mom_factor.rename(columns={mom_factor.columns[1] : 'MOM'}, inplace=True)

    return mom_factor


if __name__ == "__main__":
        
    df = famaFrench5Factor('d')

    mom = momentumFactor('d')
