from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import numpy as np
import re


_logger = logging.getLogger(__name__)


class Michigan(ScraperBase):
    REPORTING_URL = 'https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        soup = url_to_soup(self.REPORTING_URL)
        tables = soup.find_all('table')
        for table in tables:
            caption = table.find('caption')
            if caption.string.find('Confirmed COVID-19 Case') >= 0:
                m = re.search(r'updated (\d+)/(\d+)/(\d+)', caption.string)
                mon, day, year = tuple(map(int, m.groups()))
                date_published = str(datetime.date(
                    year, mon, day).strftime('%m/%d/%Y'))
                trs = table.find('tbody').find_all('tr')
                tds = trs[-1].find_all('td')
                total_cases = int(tds[1].string)
                total_deaths = int(tds[2].string)
            elif caption.string == 'Cases by Race':
                for tr in table.find('tbody').find_all('tr'):
                    tds = tr.find_all('td')
                    if tds[0].string == 'Black or African American':
                        aa_cases_pct = int(tds[1].string.strip('% '))
                        aa_deaths_pct = int(tds[2].string.strip('% '))
                        aa_cases = int(
                            np.round(total_cases * (aa_cases_pct / 100)))
                        aa_deaths = int(
                            np.round(total_deaths * (aa_deaths_pct / 100)))

        return [self._make_series(
            date=date_published,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
        )]
