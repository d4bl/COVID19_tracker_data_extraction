from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import re

_logger = logging.getLogger(__name__)


class Minnesota(ScraperBase):
    REPORTING_URL = 'https://www.health.state.mn.us/diseases/coronavirus/situation.html#raceeth1'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        soup = url_to_soup(self.REPORTING_URL)

        # find date
        strong = soup.find('strong', string=re.compile('Updated '))
        date_text = re.search(r'[A-Z][a-z][a-z]+ \d(\d)?, 20\d\d',
                              strong.text).group()

        # find total number of confirmed cases
        strong = soup.find('strong',
                           string=re.compile('Total positive( cases)?:'))
        num_cases = int(str(strong.next_sibling).strip().replace(',', ''))

        # find total number of deaths
        strong = soup.find('strong', string=re.compile('Deaths:'))
        num_deaths = int(strong.next_sibling.strip().replace(',', ''))

        date_obj = datetime.datetime.strptime(date_text, '%B %d, %Y').date()
        _logger.debug(f'Date: {date_obj}')
        _logger.debug(f'Number Cases: {num_cases}')
        _logger.debug(f'Number Deaths: {num_deaths}')

        # find number of Black/AA cases and deaths
        table = soup.find('div', attrs={'id': 'raceeth'})
        th = table.find('th', string='Black')
        if not th:
            raise ValueError('Unable to locate Black/AA data')
        tds = th.find_next_siblings('td')
        cnt_aa_cases = int(tds[0].text.strip().replace(',', ''))
        cnt_aa_deaths = int(tds[1].text.strip().replace(',', ''))
        pct_aa_cases = round(100 * cnt_aa_cases / num_cases, 2)
        pct_aa_deaths = round(100 * cnt_aa_deaths / num_deaths, 2)

        return [self._make_series(
            date=date_obj,
            cases=num_cases,
            deaths=num_deaths,
            aa_cases=cnt_aa_cases,
            aa_deaths=cnt_aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
