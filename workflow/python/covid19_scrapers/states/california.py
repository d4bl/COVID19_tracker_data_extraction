from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import string


_logger = logging.getLogger(__name__)


class California(ScraperBase):
    """California provides demographic breakdowns as tables in a web page
    updated daily.

    The table entries contain some unusual Unicode whitespace
    characters we have to handle specially.
    """

    DATA_URL = 'https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/Race-Ethnicity.aspx'
    WHITESPACE = '\u200b\u00a0' + string.whitespace

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.DATA_URL)

        # Find the update date
        date_string = soup.find(
            'div', {'class': 'NewsItemContent'}
        ).find(
            'h2', recursive=False
        ).text.strip()
        date = datetime.datetime.strptime(date_string, '%B %d, %Y').date()

        _logger.debug(f'Processing data for {date_string}')

        # Find the first table, and extract the data
        table = soup.find('table')
        cols = [th.text.strip(self.WHITESPACE)
                for th in table.find_all('th')]
        data = pd.DataFrame(
            [
                [
                    td.text.strip(self.WHITESPACE).replace(',', '')
                    for td in tr]
                for tr in table.find_all('tr')
            ][1:],
            columns=cols).set_index(
                'Race/Ethnicity'
            )

        total_cases = data.loc['Total with data', 'No. Cases']
        total_deaths = data.loc['Total with data', 'No. Deaths']
        aa_cases = data.loc['African American/Black', 'No. Cases']
        aa_cases_pct = data.loc['African American/Black',
                                'Percent Cases']
        aa_deaths = data.loc['African American/Black', 'No. Deaths']
        aa_deaths_pct = data.loc['African American/Black',
                                 'Percent Deaths']

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
