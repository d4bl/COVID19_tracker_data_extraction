from covid19_scrapers.scraper import ScraperBase

import datetime
from googleapiclient.discovery import build
import logging
import re
import pandas as pd


_logger = logging.getLogger(__name__)


class Colorado(ScraperBase):
    CO_CASE_DATA_ID = '1bBAC7H-pdEDgPxRuU_eR36ghzc0HWNf1'
    
    def __init__(self, **kwargs):
        self.api_key = kwargs['google_api_key']
        super().__init__(**kwargs)

    def _scrape(self, validation):
        service = build('drive', 'v3', developerKey=self.api_key)
        response = service.files().list(
            q=f'mimeType="text/csv" and "{self.CO_CASE_DATA_ID}" in parents and name contains "covid19_case_summary"',
            fields='files(name,webContentLink)'
        ).execute()
        if not response.get('files'):
            raise RuntimeError('Unable to find files in CO case data Google Drive')
        current_file = sorted(response['files'],
                              key=lambda x: x['name'], reverse=True)[0]

        # Find the update date
        year, month, day = map(int, re.search(
            r'covid19_case_summary_(\d{4})-(\d{2})-(\d{2})',
            current_file['name']
        ).groups())
        co_date = datetime.date(year, month, day)
        _logger.debug(f'Processing data for {co_date}')

        # Load the csv into a DataFrame
        co_data = pd.read_csv(current_file['webContentLink'])

        # Extract state totals
        state_data = co_data[
            (co_data['description'] == 'State Data') &
            (co_data['attribute'] == 'Statewide')
        ].set_index('metric')
        co_total_cases = state_data.loc['Cases', 'value']
        co_total_deaths = state_data.loc['Deaths Among Cases', 'value']

        # Extract AA percentages and compute AA totals
        aa_data = co_data[
            (co_data['description'] ==
             'COVID-19 in Colorado by Race & Ethnicity') &
            (co_data['attribute'] == 'Black - Non Hispanic')
        ].set_index('metric')

        co_aa_cases_pct = aa_data.loc['Percent of Cases', 'value']
        co_aa_cases = round(co_total_cases * co_aa_cases_pct / 100)

        co_aa_deaths_pct = aa_data.loc['Percent of Deaths', 'value']
        co_aa_deaths = round(co_total_deaths * co_aa_deaths_pct / 100)

        return [self._make_series(
            date=co_date,
            cases=co_total_cases,
            deaths=co_total_deaths,
            aa_cases=co_aa_cases,
            aa_deaths=co_aa_deaths,
            pct_aa_cases=co_aa_cases_pct,
            pct_aa_deaths=co_aa_deaths_pct,
        )]
