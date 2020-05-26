from covid19_scrapers.utils import (get_zip, get_zip_member_as_file,
                                    get_zip_member_update_date)
from covid19_scrapers.scraper import ScraperBase

import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Georgia(ScraperBase):
    GA_ZIP_URL = 'https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        _logger.debug('Download covid data zip file')
        z = get_zip(self.GA_ZIP_URL)

        _logger.debug(
            'Get the last update of the demographics.csv file in archive')
        zip_date = get_zip_member_update_date(z, 'demographics.csv')

        _logger.debug('Load demographics CSV')
        data = pd.read_csv(get_zip_member_as_file(z, 'demographics.csv'))
        by_race = data[['race', 'Confirmed_Cases', 'Deaths']
                       ].groupby('race').sum()
        totals = by_race.sum(axis=0)
        ga_cases = totals['Confirmed_Cases']
        ga_deaths = totals['Deaths']
        _logger.debug('African American cases and deaths')
        aa_key = next(filter(lambda x: x.startswith('AFRICAN-AMERICAN'), by_race.index))
        ga_aa_cases = by_race.loc[aa_key, 'Confirmed_Cases']
        ga_aa_cases_pct = round(100 * ga_aa_cases / ga_cases, 2)
        ga_aa_deaths = by_race.loc[aa_key, 'Deaths']
        ga_aa_deaths_pct = round(100 * ga_aa_deaths / ga_deaths, 2)

        return [self._make_series(
            date=zip_date,
            cases=ga_cases,
            deaths=ga_deaths,
            aa_cases=ga_aa_cases,
            aa_deaths=ga_aa_deaths,
            pct_aa_cases=ga_aa_cases_pct,
            pct_aa_deaths=ga_aa_deaths_pct,
        )]
