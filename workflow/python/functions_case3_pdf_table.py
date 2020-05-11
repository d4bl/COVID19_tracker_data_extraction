## Misc utilities
import pandas as pd
import os
from datetime import datetime, timedelta, date
#import wget
import numpy as np
import datetime

## Read webpage
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests

## Display pandas dataframe
from IPython.display import display, HTML

from misc_helper_functions import find_all_links, download_file, unzip, url_to_soup, get_json, get_metadata_date


# Import packages needed to run GA code
#import datetime
import email.utils as eut
from io import BytesIO
import re
import zipfile

import urllib.request
import requests
import ssl
import shutil
ssl._create_default_https_context = ssl._create_unverified_context


## Status code and error code
success_code = 'Success!'
error_code = 'An error occured.'





##################  extra modules needed for case 3: pdf table data extraction 

from tabula import read_pdf
from urllib.parse import urljoin


## Backwards compatibility for datetime_fromisoformat for Python 3.6 and below
## Has no effect for Python 3.7 and above
## Reference: https://pypi.org/project/backports-datetime-fromisoformat/
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()

##################  


def get_soup(url):
    r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text, 'lxml')


def get_fl_daily_url():
    fl_disaster_covid_url = 'https://floridadisaster.org/covid19/'
    fl_disaster_covid_soup = get_soup(fl_disaster_covid_url)
    daily_url = fl_disaster_covid_soup.find('a', {'title': 'COVID-19 Data - Daily Report Archive'}).get('href')
    if not daily_url:
        raise RuntimeError('Unable to find Daily Report Archive link')
    return urljoin(fl_disaster_covid_url, daily_url)


def get_fl_report_date(url):
    return date.fromisoformat(re.search(r'-(2020-\d\d-\d\d)-', fl_daily_url).group(1))

#column_number = -1




## Original parsers for Florida tables
def parse_num(val):
    if val:
        return float(val.replace(',', ''))
    return float('nan')

def parse_pct(val):
    if val:
        return float(val[:-1])/100
    return float('nan')


## New parsers used for troubleshooting
# def troubleshoot_parse_num(val):
#     print("Parsing '{}' as numeric".format(val))
#     val = val.strip().replace(',', '').replace('%', '')
#     if val:
#         return float(val)
#     return float('nan')

# def troubleshoot_parse_pct(val):
#     print("Parsing '{}' as percent".format(val))
#     val = val.strip().replace(',', '').replace('%', '')
#     if val:
#         return float(val)/100
#     return float('nan')



## These are needed for Florida

column_names = [
    'Race/ethnicity',
    'Cases', '% Cases',
    'Hospitalizations', '% Hospitalizations',
    'Deaths', '% Deaths'
]

table_area = [604, 77, 959, 561]



converters = {
    'Cases': parse_num,
    'Hospitalizations': parse_num,
    'Deaths': parse_num,
    '% Cases': parse_pct,
    '% Hospitalizations': parse_pct,
    '% Deaths': parse_pct,
}









def data_extract_florida(validation=False, home_dir = None, refresh=True):
    location_name = 'Florida'
    
    path_fl = os.path.join(home_dir, 'data', 'florida')
    os.chdir(path_fl)
    
    try:
        print('Find daily Florida URL')
        fl_daily_url = get_fl_daily_url()

        print(fl_daily_url)

        if refresh:
            download_file(fl_daily_url, 'florida.pdf')

        
        
        print('Parse the PDF')
        table = read_pdf('florida.pdf',
                        pages='3',
                        stream=True,
                        multiple_tables=False,
                        area=table_area,
                        pandas_options=dict(
                            header=None,
                            names=column_names,
                            converters=converters))[0]

        
        print('Find the report date')
        report_date = get_fl_report_date(fl_daily_url).strftime('%-m/%-d/%Y')
        
        print('Set the race/ethnicity indices')
        races = ('White', 'Black', 'Other', 'Unknown race', 'Total')
        for idx, row in table.iterrows():
            if row['Race/ethnicity'] in races:
                race = row['Race/ethnicity']
                ethnicity = 'All ethnicities'
            else:
                ethnicity = row['Race/ethnicity']
            table.loc[idx, 'Race'] = race
            table.loc[idx, 'Ethnicity'] = ethnicity
        
        table = table.drop('Race/ethnicity', axis=1)
        table = table.set_index(['Race','Ethnicity'])
        
        print('Fill NAs?')
        table.loc[('Total', 'All ethnicities')] = table.loc[('Total', 'All ethnicities')].fillna(1)

        print(table)
        

        att_names = ['Cases', 'Deaths']
        fl_all_cases_and_deaths = {nm: int(table.query("Race == 'Total' and Ethnicity == 'All ethnicities'")[nm].to_list()[0]) for nm in att_names}
        fl_aa_cases_and_deaths = {nm: int(table.query("Race == 'Black' and Ethnicity == 'Non-Hispanic'")[nm].to_list()[0]) for nm in att_names}
        fl_aa_cases_and_deaths_pct = {nm: round(100 * fl_aa_cases_and_deaths[nm] / fl_all_cases_and_deaths[nm], 2)  for nm in att_names}
        
        
        
        return {
            'Location': location_name,
            'Date Published': report_date,
            'Total Cases': fl_all_cases_and_deaths['Cases'],
            'Total Deaths': fl_all_cases_and_deaths['Deaths'],
            'Count Cases Black/AA': fl_aa_cases_and_deaths['Cases'],
            'Count Deaths Black/AA': fl_aa_cases_and_deaths['Deaths'],
            'Pct Cases Black/AA': fl_aa_cases_and_deaths_pct['Cases'],
            'Pct Deaths Black/AA': fl_aa_cases_and_deaths_pct['Deaths'],
            'Status code': success_code
        }
        
        print('Success!')
        
    except Exception as inst:
        print('Execution error!')
        print(inst)
        
        return {
        'Location': location_name,
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Count Cases Black/AA': np.nan,
        'Count Deaths Black/AA': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan,
        'Status code': "{} ... {}".format(error_code, repr(inst)) 
        }







def data_extract_san_diego(validation=False, home_dir = None):
    location_name = 'California - San Diego'
    
    path_sd = os.path.join(home_dir, 'data', 'san_diego')
    os.chdir(path_sd)
    
    try:
            
        ## Download the files
        download_file('https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Race%20and%20Ethnicity%20Summary.pdf', 'sd_cases.pdf')
        download_file('https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Deaths%20by%20Demographics.pdf', 'sd_deaths.pdf')
        
        
        ## Load the cases
        sd_cases_raw = read_pdf('sd_cases.pdf')[0]
        
        ## Format the cases
        sd_cases = sd_cases_raw.drop(index=[0,1]).reset_index().drop(columns=['index'])
        sd_cases.columns = sd_cases.loc[0]
        sd_cases = sd_cases.drop(index=[0])
        sd_cases['Count'] = [int(x.replace(',', '')) for x in sd_cases['Count']]
        sd_cases = sd_cases[['Race and Ethnicity', 'Count']]
        
        ## 
        sd_total_cases = sd_cases.Count.sum(); print(sd_total_cases)
        sd_cases['Percent'] = round(100 * sd_cases['Count'] / sd_total_cases, 2)
        
        ## 
        sd_aa_cases_cnt = sd_cases.set_index('Race and Ethnicity').loc['Black or African American','Count']
        sd_aa_cases_pct = sd_cases.set_index('Race and Ethnicity').loc['Black or African American','Percent']
        
        ##
        sd_deaths_raw = read_pdf('sd_deaths.pdf')[0]
        
        ##
        sd_deaths = sd_deaths_raw.loc[19:,:].copy().reset_index().drop(columns=['index']).dropna(how='all')
        #print(sd_deaths)
        #print(sd_deaths['San Diego County Residents'])
        sd_deaths['Count'] = [int(str(x).split()[0]) for x in sd_deaths['San Diego County Residents'] if x]
        del sd_deaths['San Diego County Residents']
        sd_deaths.columns = ['Race/Ethnicity', 'Count']
        
        ##
        sd_total_deaths = sd_deaths.Count.sum(); print(sd_total_deaths)
        sd_deaths['Percent'] = round(100 * sd_deaths['Count'] / sd_total_deaths, 2)
        
        ## 
        sd_aa_deaths_cnt = sd_deaths.set_index('Race/Ethnicity').loc['Black or African American','Count']
        sd_aa_deaths_pct = sd_deaths.set_index('Race/Ethnicity').loc['Black or African American','Percent']
        sd_max_date = (datetime.datetime.now() - timedelta(days=1)).strftime('%-m/%-d/%Y')
        
        return {
        'Location': location_name,
        'Date Published': sd_max_date,
        'Total Cases': sd_total_cases,
        'Total Deaths': sd_total_deaths,
        'Count Cases Black/AA': sd_aa_cases_cnt,
        'Count Deaths Black/AA': sd_aa_deaths_cnt,
        'Pct Cases Black/AA': sd_aa_cases_pct,
        'Pct Deaths Black/AA': sd_aa_deaths_pct,
        'Status code': success_code
        }
        


        print(success_code)
        
    except Exception as inst:
        print(error_code)
        print(inst)
        
        return {
        'Location': location_name,
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Count Cases Black/AA': np.nan,
        'Count Deaths Black/AA': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan,
        'Status code': "{} ... {}".format(error_code, repr(inst)) 
        }

















