from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class Maine(ScraperBase):
    REPORT_URL = 'https://www.maine.gov/dhhs/mecdc/infectious-disease/epi/airborne/coronavirus/data.shtml'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def __find_table(tables, title):
        """Find a table with a child whose text contains title."""
        title_re = re.compile(title)
        for table in tables:
            if table.find(text=title_re):
                return table

    def _scrape(self, **kwargs):
        # Download the data
        soup = url_to_soup(self.REPORT_URL)

        # Find the Google sheet
        url = soup.find('a', string=re.compile('Google Sheet', re.I))['href']
        url = re.sub(r'(.*)/edit.*?', r'\1/export?format=xlsx', url)
        print(f'Sheets URL is {url}')
        counties = pd.read_excel(url, sheet_name='cases_by_county')
        total_deaths = counties['DEATHS'].sum()

        table = pd.read_excel(url, sheet_name='cases_by_race', index_col=0)
        total_cases = table['CASES'].sum()
        total_cases_ex_unknown = table['CASES'].drop('Not disclosed').sum()
        date = table['DATA_REFRESH_DT'].max().date()
        aa_cases_cnt = table.loc['Black or African American', 'CASES']
        aa_cases_pct = round(100 * aa_cases_cnt / total_cases_ex_unknown, 2)

        # No race breakdowns for deaths
        aa_deaths_cnt = float('nan')
        aa_deaths_pct = float('nan')

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
