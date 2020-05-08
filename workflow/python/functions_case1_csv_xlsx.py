##### Case 1 #####

## Misc utilities
import pandas as pd
import os
from datetime import datetime, timedelta
import wget
import numpy as np
import datetime

## Read webpage
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests

## Display pandas dataframe
from IPython.display import display, HTML







######## Massachusetts

def data_extract_massachusetts(validation=False):
    ## Navigate to Massachusetts data folder
    mass_dir = os.path.join(home_dir, 'data', 'mass')
    os.chdir(mass_dir)

    try:
        print('Get URLs on Massachusetts COVID-19 response reporting page')
        mass_urls = find_all_links(url='https://www.mass.gov/info-details/covid-19-response-reporting', search_string='covid-19-raw-data')

        print('Find the URL corresponding to the COVID-19 data file')
        #print(mass_urls)
        mass_url_fragment = mass_urls[0].split('/')[2]
        mass_url = 'https://www.mass.gov/doc/{}/download'.format(mass_url_fragment)

        print('Download the file')
        ## Cumulative number of cases / deaths
        mass_file = os.path.join(mass_dir, 'massachusetts.zip'); mass_file
        os.system("wget -O {} {}".format(mass_file, mass_url))

        print('Unzip the file')
        os.system('unzip -o -qq massachusetts.zip')

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

        print('total cases')
        mass_total_cases = df_mass['All Cases'].sum()

        print('total deaths')
        mass_total_deaths = df_mass['Deaths'].sum()

        print('AA cases')
        mass_aa_cases = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['All Cases'].tolist()[0] 
        mass_aa_cases_pct = round(100 * mass_aa_cases / mass_total_cases, 2)

        print('AA deaths')
        mass_aa_deaths = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['Deaths'].tolist()[0]
        mass_aa_deaths_pct = round(100 * mass_aa_deaths / mass_total_deaths, 2)

        return {
            'Location': 'Massachusetts',
        'Date Published': mass_max_date,
        'Total Cases': mass_total_cases,
        'Total Deaths': mass_total_deaths,
        'Pct Cases Black/AA': mass_aa_cases_pct,
        'Pct Deaths Black/AA': mass_aa_deaths_pct
        }

        print('Success!')
    
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





def data_extract_virginia(validation=False):

    ## Navigate to Massachusetts data folder
    virginia_dir = os.path.join(home_dir, 'data', 'virginia')
    os.chdir(virginia_dir)

    ## No validation of 4/9/2020 available since data appear to be overwritten daily
    ## Thus, validation parameter setting has no effect
    try:
        ## Download the CSV for race
        os.system('wget -q --no-check-certificate https://www.vdh.virginia.gov/content/uploads/sites/182/2020/03/VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv')

        ## Read in the file
        df_va_raw = pd.read_csv('VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv')

        ## Get only the most recent data published
        ## TO DO: Convert date to string first before finding the max
        va_max_date = max(df_va_raw['Report Date'])

        ## Roll up counts to race
        df_va = df_va_raw.groupby('Race').sum()

        ##### Intermediate calculations #####

        ## total cases
        va_total_cases = df_va['Number of Cases'].sum()

        ## total deaths
        va_total_deaths = df_va['Number of Deaths'].sum()

        ## AA cases
        va_aa_cases = df_va.loc['Black or African American',:]['Number of Cases'] 
        va_aa_cases_pct = round(100 * va_aa_cases / va_total_cases, 2)

        ## AA deaths
        va_aa_deaths = df_va.loc['Black or African American',:]['Number of Deaths']
        va_aa_deaths_pct = round(100 * va_aa_deaths / va_total_deaths, 2)

        print('Success!')

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







def data_extract_washingtonDC(validation=False):

    ## Navigate to Washington, DC data folder
    dc_dir = os.path.join(home_dir, 'data', 'dc')
    os.chdir(dc_dir)

    try:
        ## 
        prefix = 'https://coronavirus.dc.gov/sites/default/files/dc/sites/coronavirus/page_content/attachments/'
        dc_links_raw = find_all_links('https://coronavirus.dc.gov/page/coronavirus-data', 
                    prefix + 'DC-COVID-19-Data')
        
        dc_links = [x for x in dc_links_raw if ('csv' in x or 'xlsx' in x)]
        
        ## 
        dc_date_strings = [x.replace('forApril', 'for-April'). \
                           replace(prefix + 'DC-COVID-19-Data-for-', ''). \
                           replace('-updated', '').replace('.xlsx', '') for x in dc_links]; dc_date_strings

        ##  
        dc_dates = [str(datetime.datetime.strptime(x, '%B-%d-%Y')).split(' ')[0] for x in dc_date_strings]

        ##
        dc_max_date = max(dc_dates)

        ## 
        dc_file_date = datetime.datetime.strptime(dc_max_date, '%Y-%m-%d').strftime('%B-%-d-%Y')

        ## Download the file
        ## Cumulative number of cases / deaths
        dc_url = "https://coronavirus.dc.gov/sites/default/files/dc/sites/coronavirus/page_content/attachments/DC-COVID-19-Data-for-{}.xlsx".format(dc_file_date)
        dc_file = os.path.join(dc_dir, 'dc_data.xlsx')
        os.system("wget -O {} {}".format(dc_file, dc_url))

        ## 
        df_dc_cases_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Total Cases by Race', skiprows=[0]).\
        T.drop(columns=[0])

        ##
        df_dc_cases_raw.columns = df_dc_cases_raw.loc['Unnamed: 0'].tolist()
        df_dc_cases_raw = df_dc_cases_raw.drop(index=['Unnamed: 0'])
        df_dc_cases_raw = df_dc_cases_raw.reset_index()

        ## Get date of most recent data published
        ## If desired (validation = True), verify that calculations as of D4BL's last refresh match these calculations 
        ## TO DO: Convert date to string first before finding the max
        if validation:
            max_case_ts = pd.Timestamp('2020-04-08 00:00:00')
        else:
            max_case_ts = max(df_dc_cases_raw['index']); max_case_ts

        ##
        df_dc_cases = df_dc_cases_raw[df_dc_cases_raw['index'] == max_case_ts]

        ## 
        df_dc_deaths_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Lives Lost by Race'). \
        T.drop(columns=[0])

        ## 
        df_dc_deaths_raw.columns = df_dc_deaths_raw.loc['Unnamed: 0'].tolist()
        df_dc_deaths_raw = df_dc_deaths_raw.drop(index=['Unnamed: 0'])
        df_dc_deaths_raw = df_dc_deaths_raw.reset_index()

        ##
        df_dc_deaths = df_dc_deaths_raw[df_dc_deaths_raw['index'] == max_case_ts]; df_dc_deaths

        ## 
        dc_max_date = (max_case_ts + timedelta(days=1) ).strftime('%-m/%-d/%Y'); dc_max_date

        ##### Intermediate calculations #####

        ## total cases
        dc_total_cases = df_dc_cases['All'].astype('int').tolist()[0]

        ## total deaths
        dc_total_deaths = df_dc_deaths['All'].astype('int').tolist()[0]

        ## AA cases
        dc_aa_cases = df_dc_cases['Black/African American'].astype('int').tolist()[0]
        dc_aa_cases_pct = round(100 * dc_aa_cases / dc_total_cases, 2)

        ## AA deaths
        dc_aa_deaths = df_dc_deaths['Black/African American'].astype('int').tolist()[0]
        dc_aa_deaths_pct = round(100 * dc_aa_deaths / dc_total_deaths, 2)




        print('Success!')

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

        
















