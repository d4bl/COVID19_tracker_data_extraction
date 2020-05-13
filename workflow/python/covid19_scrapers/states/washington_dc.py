from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger('covid19_scrapers')


class WashingtonDC(ScraperBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return "Washington, DC"
    
    def _scrape(self, validation):
        _logger.debug('Find links to all Washington, DC COVID data files')

        #prefix = 'https://coronavirus.dc.gov/sites/default/files/dc/sites/coronavirus/page_content/attachments/'

        ## Recent update used this prefix. Need to track if they are permanently switching over to this one
        prefix = 'https://coronavirus.dc.gov/sites/default/files/dc/sites/thrivebyfive/page_content/attachments/'
        dc_links_raw = find_all_links('https://coronavirus.dc.gov/page/coronavirus-data', 
                    prefix + 'DC-COVID-19-Data')
        
        dc_links = [x for x in dc_links_raw if ('csv' in x or 'xlsx' in x)]
        
        _logger.debug('Find date strings in data files')
        dc_date_strings = [x.replace('forApril', 'for-April'). \
                           replace(prefix + 'DC-COVID-19-Data-for-', ''). \
                           replace('-updated', '').replace('.xlsx', '')
                           for x in dc_links]
        _logger.debug(f'DC date strings: {dc_date_strings}')
        
        _logger.debug('Convert date strings to date')
        dc_dates = [str(datetime.datetime.strptime(x, '%B-%d-%Y')).split(' ')[0] for x in dc_date_strings]
        
        _logger.debug('Find most recent date')
        dc_max_date = max(dc_dates)
        
        _logger.debug('Convert to date format expected in data file')
        dc_file_date = datetime.datetime.strptime(dc_max_date, '%Y-%m-%d').strftime('%B-%d-%Y').replace('-0','-')
        
        _logger.debug('Download the most recent data file')
        ## Cumulative number of cases / deaths
        dc_url = "https://coronavirus.dc.gov/sites/default/files/dc/sites/thrivebyfive/page_content/attachments/DC-COVID-19-Data-for-{}.xlsx".format(dc_file_date)
        download_file(dc_url, 'dc_data.xlsx')
        
        _logger.debug('Load the race/ethnicity breakdown of cases')
        df_dc_cases_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Total Cases by Race', skiprows=[0]).\
        T.drop(columns=[0])
        
        _logger.debug('Set column names' )
        df_dc_cases_raw.columns = df_dc_cases_raw.loc['Unnamed: 0'].tolist()
        df_dc_cases_raw = df_dc_cases_raw.drop(index=['Unnamed: 0'])
        df_dc_cases_raw = df_dc_cases_raw.reset_index()
        
        _logger.debug('Get date of most recent data published')
        ## If desired (validation = True), verify that calculations as of D4BL's last refresh match these calculations 
        ## TO DO: Convert date to string first before finding the max
        if validation:
            max_case_ts = pd.Timestamp('2020-04-08 00:00:00')
        else:
            max_case_ts = max(df_dc_cases_raw['index'])
            _logger.debug(f'Max case timestamp: {max_case_ts}')
        
        _logger.debug('Get cases associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_cases = df_dc_cases_raw[df_dc_cases_raw['index'] == max_case_ts]
        
        _logger.debug('Load the race/ethnicity breakdown of deaths')
        df_dc_deaths_raw = pd.read_excel('dc_data.xlsx', sheet_name = 'Lives Lost by Race'). \
        T.drop(columns=[0])
        
        _logger.debug('Set column names')
        df_dc_deaths_raw.columns = df_dc_deaths_raw.loc['Unnamed: 0'].tolist()
        df_dc_deaths_raw = df_dc_deaths_raw.drop(index=['Unnamed: 0'])
        df_dc_deaths_raw = df_dc_deaths_raw.reset_index()
        
        _logger.debug('Get deaths associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_deaths = df_dc_deaths_raw[df_dc_deaths_raw['index'] == max_case_ts]; df_dc_deaths
        
        _logger.debug('Get report date, formatted for output')
        dc_max_date = (max_case_ts + timedelta(days=1) ).strftime('%m/%d/%Y'); dc_max_date
        
        ##### Intermediate calculations #####
        
        _logger.debug('Total cases')
        dc_total_cases = df_dc_cases['All'].astype('int').tolist()[0]
        
        _logger.debug('Total deaths')
        dc_total_deaths = df_dc_deaths['All'].astype('int').tolist()[0]
        
        _logger.debug('AA cases')
        dc_aa_cases = df_dc_cases['Black/African American'].astype('int').tolist()[0]
        dc_aa_cases_pct = round(100 * dc_aa_cases / dc_total_cases, 2)
        
        _logger.debug('AA deaths')
        dc_aa_deaths = df_dc_deaths['Black/African American'].astype('int').tolist()[0]
        dc_aa_deaths_pct = round(100 * dc_aa_deaths / dc_total_deaths, 2)

        return [self._make_series(
            date=dc_max_date,
            cases=dc_total_cases,
            deaths=dc_total_deaths,
            aa_cases=dc_aa_cases,
            aa_deaths=dc_aa_deaths,
            pct_aa_cases=dc_aa_cases_pct,
            pct_aa_deaths=dc_aa_deaths_pct,
        )]
