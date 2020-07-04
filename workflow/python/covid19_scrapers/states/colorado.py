import datetime
import logging
import re

from googleapiclient.discovery import build
import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Colorado(ScraperBase):
    """Colorado publishes each day's summary data as a Google Sheet, and
    adds the file to a Google Drive folder.

    This scraper uses the Google Drive API to retrieve the file name
    and contents (link).  This requires an API key to be passed in to
    run_scraper.py as an argument to the --google_api_key option.

    Note: Don't put your key in GitHub!

    To create an API key:
       1. Go to https://console.developers.google.com
       2. Click on the project selector on the top row right of
          "Google APIs"
       3. Click on NEW PROJECT on the top right of the popup, and
          choose a name, etc. (I chose "D4BL Drive Access")
       4. Click on the project selector and select your new project
       5. Click on "+ ENABLE APIS AND SERVICES" in the top center
       6. Search for and click on "Google Drive API", then click
          "ENABLE"
       7. Select on "Credentials" on the left=
       8. Click on "+ CREATE CREDENTIALS" in the top center, and
          choose "API key"
       9. Copy the key, then click on "Restrict Key"
      10. Under Application Restrictions, choose "IP addresses", and
          add your IP address to the allowed list.
      11. Under API Restrictions, choose "Restrict key" and select
          "Google Drive API".
    """

    CASE_DATA_ID = '1bBAC7H-pdEDgPxRuU_eR36ghzc0HWNf1'

    def __init__(self, **kwargs):
        self.api_key = kwargs.get('google_api_key')
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        if self.api_key is None:
            raise ValueError('Colorado scraper requires a Google API key')
        service = build('drive', 'v3', developerKey=self.api_key)
        response = service.files().list(
            q=f'mimeType="text/csv" and "{self.CASE_DATA_ID}" in parents and name contains "covid19_case_summary"',
            fields='files(name,webContentLink)'
        ).execute()
        if not response.get('files'):
            raise RuntimeError(
                'Unable to find files in CO case data Google Drive')
        current_file = sorted(response['files'],
                              key=lambda x: x['name'], reverse=True)[0]

        # Find the update date
        year, month, day = map(int, re.search(
            r'covid19_case_summary_(\d{4})-(\d{2})-(\d{2})',
            current_file['name']
        ).groups())
        date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        # Load the csv into a DataFrame
        data = pd.read_csv(current_file['webContentLink'])
        data = data[data['attribute'] != 'Note']
        data = data.set_index(['description', 'attribute', 'metric']).sort_index()

        # Extract totals
        total_cases = data.loc[('State Data', 'Statewide', 'Cases'), 'value']
        total_deaths = data.loc[('State Data', 'Statewide',
                                 'Deaths Among Cases'), 'value']

        _logger.debug(f'Total cases: {total_cases}')
        _logger.debug(f'Total deaths: {total_deaths}')
        # Extract unknown percentages and compute known totals
        unknown_cases_pct = data.loc[
            ('COVID-19 in Colorado by Race & Ethnicity',
             'Unknown/Not Provided', 'Percent of Cases'),
            'value']
        unknown_cases = round(total_cases * unknown_cases_pct / 100, 0)
        known_cases = total_cases - unknown_cases
        _logger.debug(f'Pct unknown-race cases: {unknown_cases_pct}')
        _logger.debug(f'Known-race cases: {known_cases}')

        unknown_deaths_pct = data.loc[
            ('COVID-19 in Colorado by Race & Ethnicity',
             'Unknown/Not Provided', 'Percent of Deaths'),
            'value']
        unknown_deaths = round(total_deaths * unknown_deaths_pct / 100, 0)
        known_deaths = total_deaths - unknown_deaths
        _logger.debug(f'Pct unknown-race deaths: {unknown_deaths_pct}')
        _logger.debug(f'Known-race deaths: {known_deaths}')

        # Extract AA percentages and compute AA totals
        aa_cases_pct = data.loc[
            ('COVID-19 in Colorado by Race & Ethnicity',
             'Black - Non Hispanic', 'Percent of Cases'),
            'value']
        aa_cases = round(total_cases * aa_cases_pct / 100, 0)
        aa_cases_pct = to_percentage(aa_cases, known_cases)
        _logger.debug(f'AA cases: {aa_cases}')
        _logger.debug(f'Pct AA cases: {aa_cases_pct}')

        aa_deaths_pct = data.loc[
            ('COVID-19 in Colorado by Race & Ethnicity',
             'Black - Non Hispanic', 'Percent of Deaths'),
            'value']
        aa_deaths = round(total_deaths * aa_deaths_pct / 100, 0)
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)
        _logger.debug(f'AA deaths: {aa_deaths}')
        _logger.debug(f'Pct AA deaths: {aa_deaths_pct}')

        return [self._make_series(
            date=date,
            cases=int(total_cases),
            deaths=int(total_deaths),
            aa_cases=int(aa_cases),
            aa_deaths=int(aa_deaths),
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
