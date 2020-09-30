import logging

import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Virginia(ScraperBase):
    """Virginia updates a CSV file with per-county demographic breakdowns
    of COVID-19 case and death counts.  We extract the data for the
    latest date and aggregate to the state level.
    """

    REPORTING_URL = 'https://data.virginia.gov/api/views/9sba-m86n/rows.csv?accessType=DOWNLOAD'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        _logger.debug('Read in the file')
        df_raw = pd.read_csv(self.REPORTING_URL,
                             parse_dates=['Report Date'])
        # Drop health district groups
        df_raw = df_raw[~df_raw['Health District or Health District Group'].str.contains(' and ')]

        # Aggregate by date and race/ethnicity
        df_va = df_raw.groupby(['Report Date', 'Race and Ethnicity']).sum()

        # Find last date including value for Black race/ethnicity
        df_black = df_va.loc[(slice(None), 'Black'), :]
        max_date = df_black.index.levels[0].max()
        _logger.info(f'Processing data for {max_date}')

        # Retain rows that date
        df_va = df_va.loc[max_date, :]

        # Intermediate calculations
        total_cases = df_va['Number of Cases'].sum()
        _logger.debug(f'Total cases: {total_cases}')

        total_deaths = df_va['Number of Deaths'].sum()
        _logger.debug(f'Total deaths: {total_deaths}')

        aa_cases = df_va.loc['Black', 'Number of Cases']
        _logger.debug(f'AA cases: {aa_cases}')
        aa_cases_pct = to_percentage(aa_cases, total_cases)

        aa_deaths = df_va.loc['Black', 'Number of Deaths']
        _logger.debug(f'AA deaths: {aa_deaths}')
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
            pct_includes_hispanic_black=False,
        )]
