from covid19_scrapers.utils import get_cached_url, url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import json
import logging
import re


_logger = logging.getLogger(__name__)


class CaliforniaLosAngeles(ScraperBase):
    JS_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/js/casecounter.js'
    DATA_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/locations.htm'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - Los Angeles'

    def _scrape(self, validation):
        r = get_cached_url(self.JS_URL)
        json_str = re.search(r'data = ((.|\n)*?);',
                             r.text, re.MULTILINE).group(1).strip()
        data = json.loads(json_str)['content']

        # Find the update date
        month, day, year = map(int, re.search(
            r'(\d{2})/(\d{2})/(\d{4})',
            data['info']).groups())

        date = datetime.date(year, month, day)
        _logger.debug(f'Processing data for {date}')

        # Extract the total counts
        total_cases = int(data['count'].replace(',', ''))
        total_deaths = int(data['death'].replace(',', ''))

        # Fetch the HTML page
        soup = url_to_soup(self.DATA_URL)

        # Extract the Black/AA counts
        race = soup.find(id='race')
        for tr in race.find_next_siblings('tr'):
            td = tr.find('td')
            if td and td.text.find('Black') >= 0:
                aa_cases = int(
                    td.next_sibling.text.strip().replace(',', ''))
                break

        race_d = soup.find(id='race-d')
        for tr in race_d.find_next_siblings('tr'):
            td = tr.find('td')
            if td.text.find('Black') >= 0:
                aa_deaths = int(
                    td.next_sibling.text.strip().replace(',', ''))
                break

        aa_cases_pct = round(aa_cases / total_cases * 100, 2)
        aa_deaths_pct = round(aa_deaths / total_deaths * 100, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
