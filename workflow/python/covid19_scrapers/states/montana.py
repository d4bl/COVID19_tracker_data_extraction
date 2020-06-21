from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pytz


_logger = logging.getLogger(__name__)


class Montana(ScraperBase):
    BETA_SCRAPER = True
    DATA_URL = 'https://dphhs.mt.gov/publichealth/cdepi/diseases/coronavirusmt/demographics'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.DATA_URL)

        # Find the update date
        # The headers don't include a Last-Modified, and the page does
        # not indicate when the page was updated.  As a hack based on
        # the description in the page, assume the page is modified at
        # 10AM Mountain time.
        now = datetime.datetime.now(tz=pytz.timezone('US/Mountain'))
        if now.hour >= 10:
            date = now.date()
        else:
            date = now.date() - datetime.timedelta(days=1)
        _logger.debug(f'Processing data for {date}')

        # Find the first table, and extract the data
        table = soup.find(
            'th', text='Race and Ethnicity'
        ).find_parent('table')
        aa_cases = int(table.find(
            'td', text='Black or African American'
        ).find_next_sibling('td').text.strip().split(' ')[0])
        total_cases = int(table.find(
            'td', text='Total'
        ).find_next_sibling('td').text.strip().split(' ')[0])
        aa_cases_pct = round(100 * aa_cases / total_cases, 2)

        nan = float('nan')
        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=nan,
            aa_cases=aa_cases,
            aa_deaths=nan,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=nan,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
