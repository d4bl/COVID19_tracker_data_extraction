import datetime
import logging
import re

import pytz

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import url_to_soup
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int


_logger = logging.getLogger(__name__)


class Montana(ScraperBase):
    """Montana updates a reporting page daily with demographic breakdowns
    of COVID-19 cases (not deaths yet).

    Neither the main page nor the HTTP headers include an update
    timestamp, so we guess based on the documentation that the page is
    updated "by" 10AM Mountain time.
    """

    DATA_URL = 'https://dphhs.mt.gov/publichealth/cdepi/diseases/coronavirusmt/demographics'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.DATA_URL)

        # Find the update date
        # The headers don't include a Last-Modified, and the page does
        # not indicate when the page was updated.  As a hack based on
        # the description in the page content, assume the page is modified at
        # 10AM Mountain time.
        now = datetime.datetime.now(tz=pytz.timezone('US/Mountain'))
        if now.hour >= 10:
            date = now.date()
        else:
            date = now.date() - datetime.timedelta(days=1)
        _logger.info(f'Processing data for {date}')

        # Find the summary table and extract the death count
        total_deaths = raw_string_to_int(soup.find(
            'td', string=re.compile('Number of Deaths')
        ).find_next_sibling('td').text.strip())

        # Find the demographics table and extract the data
        table = soup.find(
            'th', string=re.compile('Race and Ethnicity')
        ).find_parent('table')
        aa_cases = raw_string_to_int(table.find(
            'td', string=re.compile('Black or African American')
        ).find_next_sibling('td').text.strip().split(' ')[0])
        total_cases = raw_string_to_int(table.find(
            'td', string=re.compile('Total')
        ).find_next_sibling('td').text.strip().split(' ')[0])
        aa_cases_pct = to_percentage(aa_cases, total_cases)

        # Missing data
        nan = float('nan')
        aa_deaths = nan
        aa_deaths_pct = nan

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
