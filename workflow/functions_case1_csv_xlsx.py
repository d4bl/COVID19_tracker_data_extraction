##### Case 1 #####

## Misc utilities
import pandas as pd
import os
from datetime import datetime, timedelta
#import wget
import numpy as np
import datetime

## Read webpage
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests

## Display pandas dataframe
from IPython.display import display, HTML

from misc_helper_functions import find_all_links


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

def download_file(file_url, new_file_name=None):
    try:
        try:
            urllib.request.urlretrieve(file_url, new_file_name)
            print('file download success!')
        except:
            r = requests.get(file_url)

            with open(new_file_name, 'wb') as f:
                f.write(r.content)
                
            print('file download success!')
    except:
        print('file download failed!')
    
    


## Wrapper to unzip files
def unzip(path_to_zip_file, directory_to_extract_to='.'):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)


######## Massachusetts

def data_extract_massachusetts(validation=False, home_dir=None):

    print('Navigate to Massachusetts data folder')
    mass_dir = os.path.join(home_dir, 'data', 'mass')
    os.chdir(mass_dir)
    
    try:
        print('Get URLs on Massachusetts COVID-19 response reporting page')
        mass_urls = find_all_links(url='https://www.mass.gov/info-details/covid-19-response-reporting', search_string='covid-19-raw-data')
        print(mass_urls)

        print('Find the URL corresponding to the COVID-19 data file')
        #print(mass_urls)
        mass_url_fragment = mass_urls[0].split('/')[2]
        mass_url = 'https://www.mass.gov/doc/{}/download'.format(mass_url_fragment)
        print(mass_url)
        
        print('Download the file')
        ## Cumulative number of cases / deaths
        mass_file = os.path.join(mass_dir, 'massachusetts.zip')
        print(mass_file)
        #os.system("wget -O {} {}".format(mass_file, mass_url))
        download_file(mass_url, mass_file)
        
        print('Unzip the file')
        #os.system('unzip -o -qq massachusetts.zip')
        unzip('massachusetts.zip')
        
        print('Get the race/ethnicity breakdown')
        df_mass_raw = pd.read_csv('RaceEthnicity.csv')
        
        print('Get date of most recent data published')
        ## If desired (validation = True), verify that calculations as of D4BL's last refresh match these calculations 
        ## TO DO: Convert date to string first before finding the max
        if validation is True:
            mass_max_date = '4/9/2020'
        else:
            mass_max_date = max(df_mass_raw.Date)
        
        print('Get the data for only most recent data published (or validation date)')
        df_mass = df_mass_raw[df_mass_raw.Date == mass_max_date]
        
        ##### Intermediate calculations #####
        
        print('Total cases')
        mass_total_cases = df_mass['All Cases'].sum()
        
        print('Total deaths')
        mass_total_deaths = df_mass['Deaths'].sum()
        
        print('AA cases')
        mass_aa_cases = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['All Cases'].tolist()[0] 
        mass_aa_cases_pct = round(100 * mass_aa_cases / mass_total_cases, 2)
        
        print('AA deaths')
        mass_aa_deaths = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['Deaths'].tolist()[0]
        mass_aa_deaths_pct = round(100 * mass_aa_deaths / mass_total_deaths, 2)

        print('\nSuccess!\n')
        
        return {
            'Location': 'Massachusetts',
        'Date Published': mass_max_date,
        'Total Cases': mass_total_cases,
        'Total Deaths': mass_total_deaths,
        'Pct Cases Black/AA': mass_aa_cases_pct,
        'Pct Deaths Black/AA': mass_aa_deaths_pct
        }
        
        
    
    except Exception as inst:
        print('Execution error!')
        print(inst)
        
        return {
        'Location': 'Massachusetts',
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan
        }






def data_extract_virginia(validation=False, home_dir=None):


    print('Navigate to Virginia data folder')
    virginia_dir = os.path.join(home_dir, 'data', 'virginia')
    os.chdir(virginia_dir)
    
    ## No validation of 4/9/2020 available since data appear to be overwritten daily
    ## Thus, validation parameter setting has no effect
    try:
        print('Download the CSV for race')
        #os.system('wget -q --no-check-certificate https://www.vdh.virginia.gov/content/uploads/sites/182/2020/03/VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv')
        va_url = 'https://www.vdh.virginia.gov/content/uploads/sites/182/2020/03/VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv'
        va_file = 'VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv'
        download_file(va_url, va_file)

        print('Read in the file')
        df_va_raw = pd.read_csv('VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv')
        
        print('Get only the most recent data published')
        ## TO DO: Convert date to string first before finding the max
        va_max_date = max(df_va_raw['Report Date'])
        
        print('Roll up counts to race')
        df_va = df_va_raw.groupby('Race').sum()
        
        ##### Intermediate calculations #####
        
        print('Total cases')
        va_total_cases = df_va['Number of Cases'].sum()
        
        print('Total deaths')
        va_total_deaths = df_va['Number of Deaths'].sum()
        
        print('AA cases')
        va_aa_cases = df_va.loc['Black or African American',:]['Number of Cases'] 
        va_aa_cases_pct = round(100 * va_aa_cases / va_total_cases, 2)
        
        print('AA deaths')
        va_aa_deaths = df_va.loc['Black or African American',:]['Number of Deaths']
        va_aa_deaths_pct = round(100 * va_aa_deaths / va_total_deaths, 2)
        
        print('\nSuccess!\n')
        
        return {
            'Location': 'Virginia',
            'Date Published': va_max_date,
            'Total Cases': va_total_cases,
            'Total Deaths': va_total_deaths,
            'Pct Cases Black/AA': va_aa_cases_pct,
            'Pct Deaths Black/AA': va_aa_deaths_pct
            }
    
    except Exception as inst:
        print('Execution error!')
        print(inst)

        return {
            'Location': 'Virginia',
            'Date Published': '',
            'Total Cases': pd.nan,
            'Total Deaths': pd.nan,
            'Pct Cases Black/AA': pd.nan,
            'Pct Deaths Black/AA': pd.nan
        }







def data_extract_washingtonDC(validation=False, home_dir=None):
    
    print('Navigate to Washington, DC data folder')
    dc_dir = os.path.join(home_dir, 'data', 'dc')
    os.chdir(dc_dir)
        
    try:
        print('Find links to all Washington, DC COVID data files')

        ## This was the old prefix
        #prefix = 'https://coronavirus.dc.gov/sites/default/files/dc/sites/coronavirus/page_content/attachments/'

        ## Recent update used this prefix. Need to track if they are permanently switching over to this one
        prefix = 'https://coronavirus.dc.gov/sites/default/files/dc/sites/thrivebyfive/page_content/attachments/'
        dc_links_raw = find_all_links('https://coronavirus.dc.gov/page/coronavirus-data', 
                    prefix + 'DC-COVID-19-Data')
        
        dc_links = [x for x in dc_links_raw if ('csv' in x or 'xlsx' in x)]
        
        print('Find date strings in data files')
        dc_date_strings = [x.replace('forApril', 'for-April'). \
                           replace(prefix + 'DC-COVID-19-Data-for-', ''). \
                           replace('-updated', '').replace('.xlsx', '') for x in dc_links]; dc_date_strings
        
        print('Convert date strings to date')
        dc_dates = [str(datetime.datetime.strptime(x, '%B-%d-%Y')).split(' ')[0] for x in dc_date_strings]
        
        print('Find most recent date')
        dc_max_date = max(dc_dates)
        
        print('Convert to date format expected in data file')
        dc_file_date = datetime.datetime.strptime(dc_max_date, '%Y-%m-%d').strftime('%B-%-d-%Y')
        
        print('Download the most recent data file')
        ## Cumulative number of cases / deaths
        dc_url = "https://coronavirus.dc.gov/sites/default/files/dc/sites/thrivebyfive/page_content/attachments/DC-COVID-19-Data-for-{}.xlsx".format(dc_file_date)
        dc_file = os.path.join(dc_dir, 'dc_data.xlsx')
        #os.system("wget -O {} {}".format(dc_file, dc_url))
        download_file(dc_url, dc_file)
        
        print('Load the race/ethnicity breakdown of cases')
        df_dc_cases_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Total Cases by Race', skiprows=[0]).\
        T.drop(columns=[0])
        
        print('Set column names' )
        df_dc_cases_raw.columns = df_dc_cases_raw.loc['Unnamed: 0'].tolist()
        df_dc_cases_raw = df_dc_cases_raw.drop(index=['Unnamed: 0'])
        df_dc_cases_raw = df_dc_cases_raw.reset_index()
        
        print('Get date of most recent data published')
        ## If desired (validation = True), verify that calculations as of D4BL's last refresh match these calculations 
        ## TO DO: Convert date to string first before finding the max
        if validation:
            max_case_ts = pd.Timestamp('2020-04-08 00:00:00')
        else:
            max_case_ts = max(df_dc_cases_raw['index']); max_case_ts
        
        print('Get cases associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_cases = df_dc_cases_raw[df_dc_cases_raw['index'] == max_case_ts]
        
        print('Load the race/ethnicity breakdown of deaths')
        df_dc_deaths_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Lives Lost by Race'). \
        T.drop(columns=[0])
        
        print('Set column names')
        df_dc_deaths_raw.columns = df_dc_deaths_raw.loc['Unnamed: 0'].tolist()
        df_dc_deaths_raw = df_dc_deaths_raw.drop(index=['Unnamed: 0'])
        df_dc_deaths_raw = df_dc_deaths_raw.reset_index()
        
        print('Get deaths associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_deaths = df_dc_deaths_raw[df_dc_deaths_raw['index'] == max_case_ts]; df_dc_deaths
        
        print('Get report date, formatted for output')
        dc_max_date = (max_case_ts + timedelta(days=1) ).strftime('%-m/%-d/%Y'); dc_max_date
        
        ##### Intermediate calculations #####
        
        print('Total cases')
        dc_total_cases = df_dc_cases['All'].astype('int').tolist()[0]
        
        print('Total deaths')
        dc_total_deaths = df_dc_deaths['All'].astype('int').tolist()[0]
        
        print('AA cases')
        dc_aa_cases = df_dc_cases['Black/African American'].astype('int').tolist()[0]
        dc_aa_cases_pct = round(100 * dc_aa_cases / dc_total_cases, 2)
        
        print('AA deaths')
        dc_aa_deaths = df_dc_deaths['Black/African American'].astype('int').tolist()[0]
        dc_aa_deaths_pct = round(100 * dc_aa_deaths / dc_total_deaths, 2)
        
        
        
        
        print('\nSuccess!\n')
        
        return {
            'Location': 'Washington, DC',
            'Date Published': dc_max_date,
            'Total Cases': dc_total_cases,
            'Total Deaths': dc_total_deaths,
            'Pct Cases Black/AA': dc_aa_cases_pct,
            'Pct Deaths Black/AA': dc_aa_deaths_pct
            }
    
    except Exception as inst:
        print('Execution error!')
        print(inst)

        return {
            'Location': 'Washington, DC',
            'Date Published': '',
            'Total Cases': np.nan,
            'Total Deaths': np.nan,
            'Pct Cases Black/AA': np.nan,
            'Pct Deaths Black/AA': np.nan
        }

        




def data_extract_georgia(validation=False, home_dir=None):

    location_name = 'Georgia'
    
    print('Navigate to Georgia data folder')
    ga_dir = os.path.join(home_dir, 'data', 'ga')
    os.chdir(ga_dir)
        
    try:
        print('Download file')
        r = requests.get('https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip')

        print('Read contents of the zip')
        z = zipfile.ZipFile(BytesIO(r.content))

        print('Report date = last update of the demographics.csv file in the ZIP archive')
        info = z.getinfo('demographics.csv')
        zip_date = datetime.date(*info.date_time[0:3])
        zip_date_fmt = zip_date.strftime('%-m/%-d/%Y')

        print('Load demographics CSV')
        with z.open('demographics.csv') as cases:
            data = pd.read_csv(cases)
        by_race = data[['race', 'Confirmed_Cases', 'Deaths']].groupby('race').sum()
        totals = by_race.sum(axis=0)

        print('African American cases and deaths')
        ga_aa_cases_pct = round(100 * by_race.loc['AFRICAN-AMERICAN', 'Confirmed_Cases'] / totals['Confirmed_Cases'], 2)
        ga_aa_deaths_pct = round(100 * by_race.loc['AFRICAN-AMERICAN', 'Deaths'] / totals['Deaths'], 2)
        
        print('\nSuccess!\n')
        
        return {
            'Location': location_name,
            'Date Published': zip_date_fmt,
            'Total Cases': totals['Confirmed_Cases'],
            'Total Deaths': totals['Deaths'],
            'Pct Cases Black/AA': ga_aa_cases_pct,
            'Pct Deaths Black/AA': ga_aa_deaths_pct,
        }
    
    except Exception as inst:
        print('Execution error!')
        print(inst)

        return {
            'Location': location_name,
            'Date Published': '',
            'Total Cases': np.nan,
            'Total Deaths': np.nan,
            'Pct Cases Black/AA': np.nan,
            'Pct Deaths Black/AA': np.nan
        }













