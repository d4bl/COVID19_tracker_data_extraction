from covid19_scrapers.scraper import ScraperBase

import datetime
from googleapiclient.discovery import build
import logging
import re
import pandas as pd


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
            raise RuntimeError('Unable to find files in CO case data Google Drive')
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

        # Extract state totals
        state_data = data[
            (data['description'] == 'State Data') &
            (data['attribute'] == 'Statewide')
        ].set_index('metric')
        total_cases = state_data.loc['Cases', 'value']
        total_deaths = state_data.loc['Deaths Among Cases', 'value']

        # Extract AA percentages and compute AA totals
        aa_data = data[
            (data['description'] ==
             'COVID-19 in Colorado by Race & Ethnicity') &
            (data['attribute'] == 'Black - Non Hispanic')
        ].set_index('metric')

        aa_cases_pct = aa_data.loc['Percent of Cases', 'value']
        aa_cases = round(total_cases * aa_cases_pct / 100, 2)

        aa_deaths_pct = aa_data.loc['Percent of Deaths', 'value']
        aa_deaths = round(total_deaths * aa_deaths_pct / 100, 2)

        return [self._make_series(
            date=date,
            cases=int(total_cases),
            deaths=int(total_deaths),
            aa_cases=int(aa_cases),
            aa_deaths=int(aa_deaths),
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
