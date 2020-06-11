from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import string


_logger = logging.getLogger(__name__)


class California(ScraperBase):
    CA_DATA_URL = 'https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/Race-Ethnicity.aspx'
    WHITESPACE = '\u200b\u00a0' + string.whitespace
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        ca_soup = url_to_soup(self.CA_DATA_URL)

        # Find the update date
        date_string = ca_soup.find(
            'div', {'class': 'NewsItemContent'}
        ).find(
            'h2', recursive=False
        ).text.strip()
        ca_date = datetime.datetime.strptime(date_string, "%B %d, %Y").date()
        
        _logger.debug(f'Processing data for {date_string}')

        # Find the first table, and extract the data
        table = ca_soup.find('table')
        cols = [th.text.strip(self.WHITESPACE)
                for th in table.find_all('th')]
        _logger.info(f'Columns are {cols}')
        ca_data = pd.DataFrame(
            [
                [
                    td.text.strip(self.WHITESPACE).replace(',', '')
                    for td in tr]
                for tr in table.find_all('tr')
            ][1:],
            columns=cols).set_index(
                'Race/Ethnicity'
            )
        
        ca_total_cases = ca_data.loc['Total with data', 'No. Cases']
        ca_total_deaths = ca_data.loc['Total with data', 'No. Deaths']
        ca_aa_cases = ca_data.loc['African American/Black', 'No. Cases']
        ca_aa_cases_pct = ca_data.loc['African American/Black',
                                      'Percent Cases']
        ca_aa_deaths = ca_data.loc['African American/Black', 'No. Deaths']
        ca_aa_deaths_pct = ca_data.loc['African American/Black',
                                       'Percent Deaths']
        return [self._make_series(
            date=ca_date,
            cases=ca_total_cases,
            deaths=ca_total_deaths,
            aa_cases=ca_aa_cases,
            aa_deaths=ca_aa_deaths,
            pct_aa_cases=ca_aa_cases_pct,
            pct_aa_deaths=ca_aa_deaths_pct,
        )]
