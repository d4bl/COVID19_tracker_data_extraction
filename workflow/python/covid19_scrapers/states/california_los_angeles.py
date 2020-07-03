import datetime
import json
import logging
import re

import pandas as pd

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.utils import (
    get_cached_url, raw_string_to_int, to_percentage, url_to_soup)
from covid19_scrapers.scraper import ScraperBase


_logger = logging.getLogger(__name__)


class CaliforniaLosAngeles(ScraperBase):
    """Los Angeles publishes demographic breakdowns of COVID-19 cases and
    deaths on a county web page, but the summary data and update date
    are loaded dynamically in a script.

    We scrape this data from the script, and the demographic
    breakdowns from the main page's HTML.
    """

    JS_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/js/casecounter.js'
    DATA_URL = 'http://publichealth.lacounty.gov/media/Coronavirus/locations.htm'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - Los Angeles'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'California',
                                county='Los Angeles')

    @staticmethod
    def _extract_by_race_table(header_tr):
        data = []
        for tr in header_tr.find_next_siblings('tr'):
            td = tr.find('td')
            if not td.text.startswith('-'):
                break
            data.append([td.text.strip()[1:].strip(),
                         raw_string_to_int(td.next_sibling.text)])
        return pd.DataFrame(data, columns=['race', 'count']).set_index('race')

    def _scrape(self, **kwargs):
        r = get_cached_url(self.JS_URL)
        json_str = re.search(r'data = (([^;]|\n)*)',
                             r.text, re.MULTILINE).group(1).strip()
        # Commas on the last item in a list or object are valid in
        # JavaScript, but not in JSON.
        json_str = re.sub(r',(\s|\n)*([]}]|$)', r'\2',
                          json_str, re.MULTILINE)
        _logger.debug(f'Extracted JSON: {json_str}')
        data = json.loads(json_str)['content']

        # Find the update date
        month, day, year = map(int, re.search(
            r'(\d{2})/(\d{2})/(\d{4})',
            data['info']).groups())

        date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        # Extract the total counts
        total_cases = raw_string_to_int(data['count'])
        total_deaths = raw_string_to_int(data['death'])

        # Fetch the HTML page
        soup = url_to_soup(self.DATA_URL)

        # Extract the Black/AA counts
        cases = self._extract_by_race_table(soup.find(id='race'))
        deaths = self._extract_by_race_table(soup.find(id='race-d'))

        known_cases = cases.drop('Under Investigation')['count'].sum()
        known_deaths = deaths.drop('Under Investigation')['count'].sum()

        aa_cases = cases.loc['Black', 'count'].sum()
        aa_deaths = deaths.loc['Black', 'count'].sum()

        aa_cases_pct = to_percentage(aa_cases, known_cases)
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
