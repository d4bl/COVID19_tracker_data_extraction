from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Virginia(ScraperBase):
    REPORTING_URL = 'https://www.vdh.virginia.gov/content/uploads/sites/182/2020/03/VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv'
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def _scrape(self, validation):
        _logger.debug('Read in the file')
        df_va_raw = pd.read_csv(self.REPORTING_URL)
        
        _logger.debug('Get only the most recent data published')
        ## TO DO: Convert date to string first before finding the max
        va_max_date = max(df_va_raw['Report Date'])
        
        _logger.debug('Roll up counts to race')
        df_va = df_va_raw.groupby('Race').sum()
        
        ##### Intermediate calculations #####
        
        _logger.debug('Total cases')
        va_total_cases = df_va['Number of Cases'].sum()
        
        _logger.debug('Total deaths')
        va_total_deaths = df_va['Number of Deaths'].sum()
        
        _logger.debug('AA cases')
        va_aa_cases = df_va.loc['Black or African American',:]['Number of Cases'] 
        va_aa_cases_pct = round(100 * va_aa_cases / va_total_cases, 2)
        
        _logger.debug('AA deaths')
        va_aa_deaths = df_va.loc['Black or African American',:]['Number of Deaths']
        va_aa_deaths_pct = round(100 * va_aa_deaths / va_total_deaths, 2)

        return [self._make_series(
            date=va_max_date,
            cases=va_total_cases,
            deaths=va_total_deaths,
            aa_cases=va_aa_cases,
            aa_deaths=va_aa_deaths,
            pct_aa_cases=va_aa_cases_pct,
            pct_aa_deaths=va_aa_deaths_pct,
        )]
