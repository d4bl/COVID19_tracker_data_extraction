from covid19_scrapers.utils import (get_zip, get_zip_member_as_file,
                                    get_zip_member_update_date)
from covid19_scrapers.scraper import ScraperBase

import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Georgia(ScraperBase):
    ZIP_URL = 'https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        _logger.debug('Download covid data zip file')
        z = get_zip(self.ZIP_URL)

        _logger.debug(
            'Get the last update of the demographics.csv file in archive')
        date = get_zip_member_update_date(z, 'demographics.csv')

        _logger.debug('Load demographics CSV')
        data = pd.read_csv(get_zip_member_as_file(z, 'demographics.csv'))
        by_race = data[['race', 'Confirmed_Cases', 'Deaths']
                       ].groupby('race').sum()
        totals = by_race.sum(axis=0)
        total_cases = totals['Confirmed_Cases']
        total_deaths = totals['Deaths']
        _logger.debug('African American cases and deaths')
        aa_key = next(filter(lambda x: x.startswith('African-American'),
                             by_race.index))
        aa_cases = by_race.loc[aa_key, 'Confirmed_Cases']
        aa_cases_pct = round(100 * aa_cases / total_cases, 2)
        aa_deaths = by_race.loc[aa_key, 'Deaths']
        aa_deaths_pct = round(100 * aa_deaths / total_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
