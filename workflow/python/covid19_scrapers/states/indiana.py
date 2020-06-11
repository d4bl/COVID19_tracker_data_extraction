from covid19_scrapers.utils import get_json
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Indiana(ScraperBase):
    DATA_URL = 'https://hub.mph.in.gov/dataset/62ddcb15-bbe8-477b-bb2e-175ee5af8629/resource/2538d7f1-391b-4733-90b3-9e95cd5f3ea6/download/covid_report_demographics.xlsx'
    METADATA_URL = 'https://hub.mph.in.gov/api/3/action/resource_show?id=2538d7f1-391b-4733-90b3-9e95cd5f3ea6'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        _logger.debug('Find the update date')
        in_metadata = get_json(self.METADATA_URL)
        in_date = datetime.datetime.fromisoformat(
            in_metadata['result']['last_modified']).date()

        _logger.debug('Read in the file')
        df_in = pd.read_excel(
            self.DATA_URL,
            sheet_name='Race'
        ).set_index(
            'RACE'
        )
        
        in_total_cases = df_in['COVID_COUNT'].sum()
        in_total_deaths = df_in['COVID_DEATHS'].sum()
        in_aa_cases = df_in.loc['Black or African American', 'COVID_COUNT']
        in_aa_deaths = df_in.loc['Black or African American', 'COVID_DEATHS']
        in_aa_cases_pct = df_in.loc['Black or African American',
                                    'COVID_COUNT_PCT']
        in_aa_deaths_pct = df_in.loc['Black or African American',
                                     'COVID_DEATHS_PCT']

        return [self._make_series(
            date=in_date,
            cases=in_total_cases,
            deaths=in_total_deaths,
            aa_cases=in_aa_cases,
            aa_deaths=in_aa_deaths,
            pct_aa_cases=in_aa_cases_pct,
            pct_aa_deaths=in_aa_deaths_pct,
        )]
