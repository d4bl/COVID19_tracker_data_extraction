from covid19_scrapers.utils import get_cached_url, url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import json
import logging
import re


_logger = logging.getLogger(__name__)


class CaliforniaLosAngeles(ScraperBase):
    CA_LA_JS_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/js/casecounter.js'
    CA_LA_DATA_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/locations.htm'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - Los Angeles'

    def _scrape(self, validation):
        r = get_cached_url(self.CA_LA_JS_URL)
        ca_json = re.search(r'data = ((.|\n)*?);',
                            r.text, re.MULTILINE).group(1).strip()
        ca_data = json.loads(ca_json)['content']

        # Find the update date
        month, day, year = map(int, re.search(
            r'(\d{2})/(\d{2})/(\d{4})',
            ca_data['info']).groups())

        ca_date = datetime.date(year, month, day)
        _logger.debug(f'Processing data for {ca_date}')

        # Extract the total counts
        ca_total_cases = int(ca_data['count'].replace(',', ''))
        ca_total_deaths = int(ca_data['death'].replace(',', ''))

        # Fetch the HTML page
        ca_soup = url_to_soup(self.CA_LA_DATA_URL)

        # Extract the Black/AA counts
        race = ca_soup.find(id='race')
        for tr in race.find_next_siblings('tr'):
            td = tr.find('td')
            if td and td.text.find('Black') >= 0:
                ca_aa_cases = int(
                    td.next_sibling.text.strip().replace(',', ''))
                break

        race_d = ca_soup.find(id='race-d')
        for tr in race_d.find_next_siblings('tr'):
            td = tr.find('td')
            if td.text.find('Black') >= 0:
                ca_aa_deaths = int(
                    td.next_sibling.text.strip().replace(',', ''))
                break

        ca_aa_cases_pct = round(ca_aa_cases / ca_total_cases * 100, 2)
        ca_aa_deaths_pct = round(ca_aa_deaths / ca_total_deaths * 100, 2)

        return [self._make_series(
            date=ca_date,
            cases=ca_total_cases,
            deaths=ca_total_deaths,
            aa_cases=ca_aa_cases,
            aa_deaths=ca_aa_deaths,
            pct_aa_cases=ca_aa_cases_pct,
            pct_aa_deaths=ca_aa_deaths_pct,
        )]
