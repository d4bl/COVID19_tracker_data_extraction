import datetime
import logging
import re
import string

from covid19_scrapers.utils.html import table_to_dataframe, url_to_soup
from covid19_scrapers.utils.parse import raw_string_to_int
from covid19_scrapers.scraper import ScraperBase


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

        _logger.info(f'Processing data for {date}')

        # Find the first table, and extract the data
        table = soup.find('table')
        data = table_to_dataframe(table).set_index('Race/Ethnicity')
        black_lbl = data.index[data.index.str.contains('Black')]
        assert len(black_lbl) == 1, f'Unexpected "Black" label in table: {black_lbl}'
        black_lbl = black_lbl[0]
        _logger.debug(f'Black label: {black_lbl}')
        known_cases = data.loc['Total with data', 'No. Cases']
        known_deaths = data.loc['Total with data', 'No. Deaths']
        aa_cases = data.loc[black_lbl, 'No. Cases']
        aa_cases_pct = data.loc[black_lbl, 'Percent Cases']
        aa_deaths = data.loc[black_lbl, 'No. Deaths']
        aa_deaths_pct = data.loc[black_lbl, 'Percent Deaths']

        cases_soup = table.find_next('h4', text=re.compile('Cases:'))
        total_cases = raw_string_to_int(
            re.search(r'Cases:\s+([0-9,]+)\s+total', cases_soup.text).group(1))

        deaths_soup = table.find_next('h4', text=re.compile('Deaths:'))
        total_deaths = raw_string_to_int(
            re.search(r'Deaths:\s+([0-9,]+)\s+total', deaths_soup.text).group(1))
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
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
