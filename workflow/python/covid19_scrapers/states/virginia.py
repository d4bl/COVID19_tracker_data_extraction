from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import to_percentage

import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Virginia(ScraperBase):
    """Virginia updates a CSV file with per-county demographic breakdowns
    of COVID-19 case and death counts.  We extract the data for the
    latest date and aggregate to the state level.
    """

    REPORTING_URL = 'https://www.vdh.virginia.gov/content/uploads/sites/182/2020/03/VDH-COVID-19-PublicUseDataset-Cases_By-Race.csv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        _logger.debug('Read in the file')
        df_raw = pd.read_csv(self.REPORTING_URL,
                             parse_dates=['Report Date'])

        _logger.debug('Get only the most recent data published')
        # TO DO: Convert date to string first before finding the max
        max_date = max(df_raw['Report Date']).date()
        _logger.info(f'Processing data for {max_date}')

        _logger.debug('Roll up counts to race')
        df_va = df_raw.groupby('Race').sum()

        # Intermediate calculations

        _logger.debug('Total cases')
        total_cases = df_va['Number of Cases'].sum()

        _logger.debug('Total deaths')
        total_deaths = df_va['Number of Deaths'].sum()

        _logger.debug('AA cases')
        aa_cases = df_va.loc['Black or African American',
                             :]['Number of Cases']
        aa_cases_pct = to_percentage(aa_cases, total_cases)

        _logger.debug('AA deaths')
        aa_deaths = df_va.loc['Black or African American',
                              :]['Number of Deaths']
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
