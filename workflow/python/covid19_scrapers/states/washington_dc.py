from covid19_scrapers.utils import download_file, find_all_links
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import re
from urllib.parse import urljoin, urlsplit

_logger = logging.getLogger(__name__)


class WashingtonDC(ScraperBase):
    DC_MAIN_URL = 'https://coronavirus.dc.gov/page/coronavirus-data'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Washington, DC'

    def _scrape(self, validation):
        _logger.debug('Find links to all Washington, DC COVID data files')

        prefix = re.compile(r'/sites/default/' +
                            r'files/dc/sites/(coronavirus|thrivebyfive)/' +
                            r'page_content/attachments/DC-COVID-19-Data')
        dc_links_raw = find_all_links(self.DC_MAIN_URL, prefix)

        dc_links = [x for x in dc_links_raw if ('csv' in x or 'xlsx' in x)]

        _logger.debug('Find date strings in data files')
        date_extractor = re.compile(r'for-?([A-Z][a-z]+-\d+-\d+)')
        dc_max_date_link = max(dc_links,
                               key=lambda url: datetime.datetime.strptime(
                                   date_extractor.search(url).group(1),
                                   '%B-%d-%Y').date())
        _logger.debug(f'Most recent dated link: {dc_max_date_link}')

        _logger.debug('Download the most recent data file')
        # Cumulative number of cases / deaths
        if urlsplit(dc_max_date_link).scheme:
            dc_url = dc_max_date_link
        else:
            dc_url = urljoin(self.DC_MAIN_URL, dc_max_date_link)
        download_file(dc_url, 'dc_data.xlsx', force_remote=True)

        _logger.debug('Load the race/ethnicity breakdown of cases')
        df_dc_cases_raw = pd.read_excel(
            'dc_data.xlsx', sheet_name='Total Cases by Race', skiprows=[0]
        ).T.drop(columns=[0])

        _logger.debug('Set column names')
        df_dc_cases_raw.columns = df_dc_cases_raw.loc['Unnamed: 0'].tolist()
        df_dc_cases_raw = df_dc_cases_raw.drop(index=['Unnamed: 0'])
        df_dc_cases_raw = df_dc_cases_raw.reset_index()

        _logger.debug('Get date of most recent data published')
        # If desired (validation = True), verify that calculations as
        # of D4BL's last refresh match these calculations
        # TO DO: Convert date to string first before finding the max
        if validation:
            max_case_ts = pd.Timestamp('2020-04-08 00:00:00')
        else:
            max_case_ts = max(pd.to_datetime(df_dc_cases_raw['index']))
            _logger.debug(df_dc_cases_raw['index'])
            _logger.debug(f'Max case timestamp: {max_case_ts}')

        _logger.debug(
            'Get cases associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_cases = df_dc_cases_raw[df_dc_cases_raw['index'] == max_case_ts]

        _logger.debug('Load the race/ethnicity breakdown of deaths')
        df_dc_deaths_raw = pd.read_excel(
            'dc_data.xlsx', sheet_name='Lives Lost by Race'
        ).T.drop(columns=[0])

        _logger.debug('Set column names')
        df_dc_deaths_raw.columns = df_dc_deaths_raw.loc['Unnamed: 0'].tolist()
        df_dc_deaths_raw = df_dc_deaths_raw.drop(index=['Unnamed: 0'])
        df_dc_deaths_raw = df_dc_deaths_raw.reset_index()

        _logger.debug(
            'Get deaths associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_dc_deaths = df_dc_deaths_raw[df_dc_deaths_raw['index']
                                        == max_case_ts]
        df_dc_deaths

        _logger.debug('Get report date')
        dc_max_date = (
            max_case_ts + datetime.timedelta(days=1)
        )

        ##### Intermediate calculations #####

        _logger.debug('Total cases')
        dc_total_cases = df_dc_cases['All'].astype('int').tolist()[0]

        _logger.debug('Total deaths')
        dc_total_deaths = df_dc_deaths['All'].astype('int').tolist()[0]

        _logger.debug('AA cases')
        dc_aa_cases = df_dc_cases['Black/African American'].astype('int').tolist()[
            0]
        dc_aa_cases_pct = round(100 * dc_aa_cases / dc_total_cases, 2)

        _logger.debug('AA deaths')
        dc_aa_deaths = df_dc_deaths[
            'Black/African American'
        ].astype('int').tolist()[0]
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
