import datetime
import logging
import pandas as pd
import re
from urllib.parse import urljoin, urlsplit

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import (
    download_file, find_all_links, to_percentage)


_logger = logging.getLogger(__name__)


class WashingtonDC(ScraperBase):
    """Washington, DC, publishes separate spreadsheets (CSV or Excel
    depending on the date) for each day's COVID-19 demographic
    disaggregations.  We find the latest URL from their main reporting
    page, and retrieve the contents as a DataFrame.
    """

    MAIN_URL = 'https://coronavirus.dc.gov/page/coronavirus-data'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Washington, DC'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'District of Columbia')

    def _scrape(self, *, validation=False, **kwargs):
        _logger.debug('Find links to all Washington, DC COVID data files')

        prefix = re.compile(r'/sites/default/'
                            + r'files/dc/sites/(coronavirus|thrivebyfive)/'
                            + r'page_content/attachments/DC-COVID-19-Data')
        links_raw = find_all_links(self.MAIN_URL, prefix)

        links = [x for x in links_raw if ('csv' in x or 'xlsx' in x)]

        _logger.debug('Find date strings in data files')
        date_extractor = re.compile(r'for-?([A-Z][a-z]+-\d+-\d+)')
        max_date_link = max(links,
                            key=lambda url: datetime.datetime.strptime(
                                date_extractor.search(url).group(1),
                                '%B-%d-%Y').date())
        _logger.debug(f'Most recent dated link: {max_date_link}')

        _logger.debug('Download the most recent data file')
        # Cumulative number of cases / deaths
        if urlsplit(max_date_link).scheme:
            url = max_date_link
        else:
            url = urljoin(self.MAIN_URL, max_date_link)
        download_file(url, 'data.xlsx', force_remote=True)

        _logger.debug('Load the race/ethnicity breakdown of cases')
        df_cases_raw = pd.read_excel(
            'data.xlsx', sheet_name='Total Cases by Race', skiprows=[0]
        ).T.drop(columns=[0])

        _logger.debug('Set column names')
        df_cases_raw.columns = df_cases_raw.loc['Unnamed: 0'].tolist()
        df_cases_raw = df_cases_raw.drop(index=['Unnamed: 0'])
        df_cases_raw = df_cases_raw.reset_index()

        _logger.debug('Get date of most recent data published')
        # If desired (validation = True), verify that calculations as
        # of D4BL's last refresh match these calculations
        # TO DO: Convert date to string first before finding the max
        if validation:
            max_case_ts = pd.Timestamp('2020-04-08 00:00:00')
        else:
            max_case_ts = max(pd.to_datetime(df_cases_raw['index']))
            _logger.debug(f'Max case timestamp: {max_case_ts}')

        _logger.debug(
            'Get cases associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_cases = df_cases_raw[df_cases_raw['index'] == max_case_ts]

        _logger.debug('Load the race/ethnicity breakdown of deaths')
        df_deaths_raw = pd.read_excel(
            'data.xlsx', sheet_name='Lives Lost by Race'
        ).T.drop(columns=[0])

        _logger.debug('Set column names')
        df_deaths_raw.columns = df_deaths_raw.loc['Unnamed: 0'].tolist()
        df_deaths_raw = df_deaths_raw.drop(index=['Unnamed: 0'])
        df_deaths_raw = df_deaths_raw.reset_index()

        _logger.debug(
            'Get deaths associated with desired timestamp (most recent or 4/9/2020 validation date)')
        df_deaths = df_deaths_raw[df_deaths_raw['index'] == max_case_ts]

        max_date = (
            max_case_ts + datetime.timedelta(days=1)
        ).date()
        _logger.info('Processing report for {date}')

        # Intermediate calculations

        _logger.debug('Total cases')
        total_cases = df_cases['All'].astype('int').tolist()[0]

        _logger.debug('Total deaths')
        total_deaths = df_deaths['All'].astype('int').tolist()[0]

        _logger.debug('AA cases')
        aa_cases = df_cases['Black/African American'].astype('int').tolist()[
            0]
        aa_cases_pct = to_percentage(aa_cases, total_cases)

        _logger.debug('AA deaths')
        aa_deaths = df_deaths[
            'Black/African American'
        ].astype('int').tolist()[0]
        aa_deaths_pct = to_percentage(aa_deaths, total_deaths)

        return [self._make_series(
            date=max_date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
