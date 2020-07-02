from covid19_scrapers.utils import to_percentage, url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import re
from urllib.parse import urljoin


_logger = logging.getLogger(__name__)


class Michigan(ScraperBase):
    """Michigan updates a reporting page daily with demographic breakdowns
    of cases and deaths. We scrape the page for update date and data.
    """

    REPORTING_URL = 'https://www.michigan.gov/coronavirus/0,9753,7-406-98163_98173---,00.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Find latest report
        soup = url_to_soup(self.REPORTING_URL)
        by_dem_path = soup.find(
            'a',
            text='Cases by Demographics Statewide')['href']

        # Extract the report date
        (year, month, day) = map(int, re.search(
            r'(\d{4})-(\d{2})-(\d{2})', by_dem_path).groups())

        date_published = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date_published}')

        # Load the data
        by_dem_url = urljoin(self.REPORTING_URL, by_dem_path)
        by_dem = pd.read_excel(by_dem_url)

        # Drop probable cases
        by_dem = by_dem[by_dem['CASE_STATUS'] == 'Confirmed']
        by_dem['Cases'] = by_dem['Cases'].str.replace(
            'Suppressed', '0').astype(int)
        by_dem['Deaths'] = by_dem['Deaths'].str.replace(
            'Suppressed', '0').astype(int)
        by_race = by_dem[['RaceCat', 'Cases', 'Deaths']].groupby(
            'RaceCat').sum()

        total = by_race.sum(axis=0)
        total_cases = total['Cases']
        total_deaths = total['Deaths']
        aa_cases = by_race.loc['Black/African American', 'Cases']
        aa_cases_pct = to_percentage(aa_cases, total_cases)
        aa_deaths = by_race.loc['Black/African American', 'Deaths']
        aa_deaths_pct = to_percentage(aa_deaths, total_deaths)

        return [self._make_series(
            date=date_published,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
