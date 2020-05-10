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



#import datetime fix



def data_extract_michigan(validation=False, home_dir=None):

    try:
            # # Michigan
        MI = {
             'Location': 'Michigan'
        }

        MI_url = "https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html"
        MI_soup = url_to_soup(MI_url)
        tables = MI_soup.find_all('table')
        for table in tables:
            caption = table.find('caption')
            if caption.string.find('Confirmed COVID-19 Case') >= 0:
                m = re.search('updated (\d+)/(\d+)/(\d+)', caption.string)
                mon, day, year = tuple(map(int, m.groups()))
                MI['Date Published'] = str(datetime.date(year, mon, day).strftime('%-m/%-d/%Y'))
                trs = table.find('tbody').find_all('tr')
                tds = trs[-1].find_all('td')
                total_cases = int(tds[1].string)
                total_deaths = int(tds[2].string)
            elif caption.string == 'Cases by Race':
                for tr in table.find('tbody').find_all('tr'):
                    tds = tr.find_all('td')
                    if tds[0].string == 'Black or African American':
                        aa_cases_pct = int(tds[1].string.strip('% '))
                        aa_deaths_pct = int(tds[2].string.strip('% '))
                        aa_cases = int(np.round(total_cases * (aa_cases_pct / 100)))
                        aa_deaths = int(np.round(total_deaths * (aa_deaths_pct / 100)))
        MI['Total Cases'] = total_cases
        MI['Total Deaths'] = total_deaths
        MI['Count Cases Black/AA'] = aa_cases
        MI['Count Deaths Black/AA'] = aa_deaths
        MI['Pct Cases Black/AA'] = aa_cases_pct
        MI['Pct Deaths Black/AA'] = aa_deaths_pct
        
        print(success_code)

        MI['Status code'] = success_code
        
        return MI

    except Exception as inst:
        print(error_code)
        print(inst)
        
        return {
        'Location': 'Michigan',
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Count Cases Black/AA': np.nan,
        'Count Deaths Black/AA': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan,
        'Status code': "{} ... {}".format(error_code, repr(inst)) 
        }







def data_extract_minnesota(validation=False, home_dir=None):

    try:
        MN_url = "https://www.health.state.mn.us/diseases/coronavirus/situation.html#raceeth1"
        MN_soup = url_to_soup(MN_url)
        
        # find date and total number of cases and deaths
        counter = 0
        num_cases = ''
        num_deaths = ''
        for strong_tag in MN_soup.find_all('strong'):
            this_string = strong_tag.text, strong_tag.next_sibling
            this_heading = strong_tag.text
            if counter == 0:
                date_text = strong_tag.text.strip('.')[11:]
            if this_heading == 'Total positive: ':
                num_cases = strong_tag.next_sibling
                num_cases = int(num_cases.replace(',', ''))
            if this_heading == 'Deaths: ':
                num_deaths = strong_tag.next_sibling
                num_deaths = int(num_deaths.replace(',', ''))
            counter += 1
            
        date_time_obj = datetime.datetime.strptime(date_text, "%B %d, %Y")
        date_formatted = date_time_obj.strftime("%-m/%-d/%Y")
        print('Date:', date_formatted)
        print('Number Cases:', num_cases)
        print('Number Deaths:', num_deaths)
        
        # find number of Black/AA cases and deaths
        table = MN_soup.find("div", attrs={"id":"raceeth"})
        counter = 0
        pct_cases = ''
        pct_deaths = ''
        for th in table.find_all('th'):
            text = th.text
        #     print(th.next_sibling)
            if text == "Black":
        #         print(table.find_all('td'))
                pct_cases_txt = table.find_all('td')[counter-2].text.strip('%')
                pct_deaths_txt = table.find_all('td')[counter-1].text.strip('%')
                pct_aa_cases = int(pct_cases_txt)
                pct_aa_deaths = int(pct_deaths_txt)
                cnt_aa_cases = int(np.round(num_cases * (pct_aa_cases / 100)))
                cnt_aa_deaths = int(np.round(num_deaths * (pct_aa_deaths / 100)))

            counter += 1
        
        print('Pct Cases Black/AA:', pct_cases)
        print('Pct Deaths Black/AA:', pct_deaths)
        
        print(success_code)
        
        return {
            'Location': 'Minnesota',
            'Date Published': date_formatted,
            'Total Cases': num_cases,
            'Total Deaths': num_deaths,
            'Count Cases Black/AA': cnt_aa_cases,
            'Count Deaths Black/AA': cnt_aa_deaths,
            'Pct Cases Black/AA': pct_aa_cases,
            'Pct Deaths Black/AA': pct_aa_deaths,
            'Status code': success_code
        }
        
    except Exception as inst:
        print(error_code)
        print(inst)
        
        return {
        'Location': 'Minnesota',
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Count Cases Black/AA': np.nan,
        'Count Deaths Black/AA': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan,
        'Status code': "{} ... {}".format(error_code, repr(inst)) 
        }












def data_extract_north_carolina(validation=False, home_dir=None):
    
    try:
        NC_url = "https://www.ncdhhs.gov/divisions/public-health/covid19/covid-19-nc-case-count#by-race-ethnicity"
        NC_soup = url_to_soup(NC_url)
        
        # find date and total number of cases and deaths
        date_text = NC_soup.find("div", attrs={"class":"field-item"}).p.text[50:]
        date_time_obj = datetime.datetime.strptime(date_text, "%B %d, %Y. ")
        date_formatted = date_time_obj.strftime("%-m/%-d/%Y")
        
        field_item = NC_soup.find("div", attrs={"class":"field-item"})
        # num_cases = field_item.findAll("tr")[1].td.text
        items = field_item.findAll("tr")[1]
        num_cases = int(items.findAll("td")[1].text.replace(',', ''))
        num_deaths = int(items.findAll("td")[0].text.replace(',', ''))
        
        print('Date:', date_formatted)
        print('Number Cases:', num_cases)
        print('Number Deaths:', num_deaths)
        
        # find number of Black/AA cases and deaths
        tables = NC_soup.findAll("table")
        race_data = tables[4]
        num_race_cases = race_data.findAll("td")[6]
        num_race_deaths = race_data.findAll("td")[8]
        pct_aa_cases = int(race_data.findAll("td")[22].text.strip('%'))
        pct_aa_deaths = int(race_data.findAll("td")[24].text.strip('%'))
        cnt_aa_cases = int(np.round(num_cases * (pct_aa_cases / 100)))
        cnt_aa_deaths = int(np.round(num_deaths * (pct_aa_deaths / 100))) 
        
        print('Pct Cases Black/AA:', pct_aa_cases)
        print('Pct Deaths Black/AA:', pct_aa_deaths)
        
        print(success_code)
        
        return {
            'Location': 'North Carolina',
            'Date Published': date_formatted,
            'Total Cases': num_cases,
            'Total Deaths': num_deaths,
            'Count Cases Black/AA': cnt_aa_cases,
            'Count Deaths Black/AA': cnt_aa_deaths,
            'Pct Cases Black/AA': int(pct_aa_cases),
            'Pct Deaths Black/AA': int(pct_aa_deaths),
            'Status code': success_code
        }
        
    except Exception as inst:
        print(error_code)
        print(inst)
        
        return {
        'Location': 'North Carolina',
        'Date Published': '',
        'Total Cases': np.nan,
        'Total Deaths': np.nan,
        'Count Cases Black/AA': np.nan,
        'Count Deaths Black/AA': np.nan,
        'Pct Cases Black/AA': np.nan,
        'Pct Deaths Black/AA': np.nan,
        'Status code': "{} ... {}".format(error_code, repr(inst)) 
        }








def data_extract_texas_bexar_county(validation=False, home_dir=None):
    location_name = 'Texas -- Bexar County'
        
    TX_Bexar = {
    'Location': location_name,
    }
    
    placeholder_output = {
            'Location': location_name,
            'Date Published': '',
            'Total Cases': np.nan,
            'Total Deaths': np.nan,
            'Count Cases Black/AA': np.nan,
            'Count Deaths Black/AA': np.nan,
            'Pct Cases Black/AA': np.nan,
            'Pct Deaths Black/AA': np.nan
            }
    

    try:
        # Start by fetching the metadata to get the likey timestamp
        md_date = get_metadata_date('https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0?f=json')
        TX_Bexar['Date Published'] = str(md_date.strftime('%-m/%-d/%Y'))
        
        # Next get the cumulative case and death counts
        total = get_json('https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vDateCOVID19_Tracker_Public/FeatureServer/0/query?f=json&where=Date%20BETWEEN%20timestamp%20%272020-05-07%2005%3A00%3A00%27%20AND%20timestamp%20%272020-05-08%2004%3A59%3A59%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true')
        TX_Bexar['Total Cases'] = total['features'][0]['attributes']['ReportedCum']
        TX_Bexar['Total Deaths'] = total['features'][0]['attributes']['DeathsCum']
        
        # And finally the race/ethnicity breakdowns
        data = get_json('https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true')
        for feature in data.get('features', []):
            if feature['attributes']['RaceEthnicity'] == 'Black':
                TX_Bexar['Count Cases Black/AA'] = feature['attributes']['CasesConfirmed']
                TX_Bexar['Count Deaths Black/AA'] = feature['attributes']['Deaths']
                TX_Bexar['Pct Cases Black/AA'] = round(100 * feature['attributes']['CasesConfirmed'] / TX_Bexar['Total Cases'], 2)
                TX_Bexar['Pct Deaths Black/AA'] = round(100 * feature['attributes']['Deaths'] / TX_Bexar['Total Deaths'], 2)
                break
        if 'Pct Cases Black/AA' not in TX_Bexar:
            raise ValueError('No data found for Black RaceEthnicity category')
        
        print(success_code)

        TX_Bexar['Status code'] = success_code

        return TX_Bexar
        
    except OverflowError as e:
        print("Error processing last update timstamp for TX_Bexar")
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        return placeholder_output
    except ValueError as e:
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        print("Error processing data for TX_Bexar", e)
        return placeholder_output
    except requests.RequestException as e:
        print("Error retrieving URL for TX_Bexar:", e.request.url)
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        return placeholder_output
    


def data_extract_wisconsin_milwaukee(validation=False, home_dir=None):
    
    location_name = 'Wisconsin -- Milwaukee'
    
    WI_Milwaukee = {
        'Location': location_name,
    }
    
    placeholder_output = {
            'Location': location_name,
            'Date Published': '',
            'Total Cases': np.nan,
            'Total Deaths': np.nan,
            'Count Cases Black/AA': np.nan,
            'Count Deaths Black/AA': np.nan,
            'Pct Cases Black/AA': np.nan,
            'Pct Deaths Black/AA': np.nan 
            }
    
    try:
        # Get the timestamp
        cases_date = get_metadata_date('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0?f=json')
        deaths_date = get_metadata_date('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0?f=json')
        if cases_date != deaths_date:
            print('Unexpected mismath between cases and deaths metadata dates:', cases_date, '!=', deaths_date)
        WI_Milwaukee['Date Published'] = str(cases_date.strftime('%-m/%-d/%Y'))
        
        cases_total = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        WI_Milwaukee['Total Cases'] = cases_total['features'][0]['attributes']['value']
        deaths_total = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        WI_Milwaukee['Total Deaths'] = deaths_total['features'][0]['attributes']['value']
        
        cases_by_race = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=Race_Eth%20NOT%20LIKE%20%27%25%23N%2FA%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        for feature in cases_by_race['features']:
            if feature['attributes']['Race_Eth'] == 'Black Alone':
                WI_Milwaukee['Count Cases Black/AA'] = feature['attributes']['value']
                WI_Milwaukee['Pct Cases Black/AA'] = round(100 * feature['attributes']['value'] / WI_Milwaukee['Total Cases'], 2)
                break
        
        deaths_by_race = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        for feature in deaths_by_race['features']:
            if feature['attributes']['Race_Eth'] == 'Black Alone':
                WI_Milwaukee['Count Deaths Black/AA'] = feature['attributes']['value']
                WI_Milwaukee['Pct Deaths Black/AA'] = round(100 * feature['attributes']['value'] / WI_Milwaukee['Total Deaths'], 2)
                break
        
        print(success_code)
        WI_Milwaukee['Status code'] = success_code
        return WI_Milwaukee
        
    except OverflowError as e:
        print("Error processing last update timstamp for WI_Milwaukee")
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        return placeholder_output
        
    except ValueError as e:
        print("Error processing data for WI_Milwaukee", e)
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        return placeholder_output
        
    except requests.RequestException as e:
        print("Error retrieving URL for WI_Milwaukee:", e.request.url)
        placeholder_output['Status code'] = "{} ... {}".format(error_code, repr(e)) 
        return placeholder_output








