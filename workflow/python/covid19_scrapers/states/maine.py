from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import re


_logger = logging.getLogger(__name__)


class Maine(ScraperBase):
    BETA_SCRAPER = True
    REPORT_URL = 'https://www.maine.gov/dhhs/mecdc/infectious-disease/epi/airborne/coronavirus.shtml'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def __find_table(tables, title):
        """Find a table with a child whose text contains title."""
        title_re = re.compile(title)
        for table in tables:
            if table.find(text=title_re):
                return table

    def _scrape(self, validation):
        # Download the data
        soup = url_to_soup(self.REPORT_URL)

        # Find the summary table
        tables = soup.find_all('table', class_='travelAdvisories')
        table = self.__find_table(tables, 'Cumulative Case Data')

        # Extract the date
        date_str = re.search(
            r'[A-Z][a-z]+ \d+, \d+',
            table.find('th', class_='advisoryDt').text).group(0)
        date = datetime.datetime.strptime(date_str, '%B %d, %Y').date()
        _logger.info(f'Processing data for {date}')

        # Extract the case and death totals
        tbody = table.find('tbody')
        tr = tbody.find_all('tr')[1]
        vals = list(map(int,
                        [td.text.replace(',', '').strip()
                         for td in tr.find_all('td')]))
        total_cases = vals[0]
        total_deaths = vals[-1]

        # Extract the AA data
        table = self.__find_table(tables, 'Cases by Race')
        for tr in table.find_all('tr')[1:]:
            if tr.find('td').text.find('Black') >= 0:
                tds = tr.find_all('td')
                td = tds[1]
                aa_cases_cnt = int(td.text.replace(',', '').strip())
                aa_cases_pct = round(100 * aa_cases_cnt / total_cases, 2)
                break

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
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
