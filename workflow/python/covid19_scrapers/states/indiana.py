import datetime
import logging
import pandas as pd

from covid19_scrapers.utils.http import get_json
from covid19_scrapers.scraper import ScraperBase


_logger = logging.getLogger(__name__)


class Indiana(ScraperBase):
    """Indiana lists their COVID-19 case demographics dataset on their
    state health department's open data portal:
    https://hub.mph.in.gov/dataset/62ddcb15-bbe8-477b-bb2e-175ee5af8629

    This includes both the data as an Excel file and a data dictionary.
    """

    DATA_URL = 'https://hub.mph.in.gov/dataset/62ddcb15-bbe8-477b-bb2e-175ee5af8629/resource/2538d7f1-391b-4733-90b3-9e95cd5f3ea6/download/covid_report_demographics.xlsx'
    METADATA_URL = 'https://hub.mph.in.gov/api/3/action/resource_show?id=2538d7f1-391b-4733-90b3-9e95cd5f3ea6'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        _logger.debug('Find the update date')
        metadata = get_json(self.METADATA_URL)
        date = datetime.datetime.fromisoformat(
            metadata['result']['last_modified']).date()
        _logger.info(f'Processing data for {date}')

        _logger.debug('Read in the file')
        df_in = pd.read_excel(
            self.DATA_URL,
            sheet_name='Race'
        ).set_index(
            'RACE'
        )

        total_cases = df_in['COVID_COUNT'].sum()
        total_deaths = df_in['COVID_DEATHS'].sum()
        aa_cases = df_in.loc['Black or African American', 'COVID_COUNT']
        aa_deaths = df_in.loc['Black or African American', 'COVID_DEATHS']
        aa_cases_pct = df_in.loc['Black or African American',
                                 'COVID_COUNT_PCT']
        aa_deaths_pct = df_in.loc['Black or African American',
                                  'COVID_DEATHS_PCT']

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
