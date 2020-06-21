from covid19_scrapers.utils import table_to_dataframe, url_to_soup
from covid19_scrapers.scraper import ScraperBase

from bs4 import BeautifulSoup
import datetime
import json
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class Utah(ScraperBase):
    DATA_URL = 'https://coronavirus-dashboard.utah.gov/index.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.DATA_URL)
        soup.find()(id='demographics')

        # Extract publication date
        overview = soup.find(id='overview-of-covid-19-surveillance')
        date_str = re.search(r'Report Date: ([A-Za-z]+ \d+, \d+)',
                             overview.text).group(1)
        date = datetime.datetime.strptime(date_str, '%B %d, %Y').date()
        _logger.info(f'Processing data for {date}')

        # Extract demographic and total data
        race_data = json.loads(
            soup.find(
                id='cases-hospitalizations-and-deaths-by-raceethnicity'
            ).find('script', {'type': 'application/json'}).string)

        headers = [th.string.strip()
                   for th in BeautifulSoup(
                           race_data['x']['container'],
                           features='lxml').find_all('th')]
        race_df = pd.DataFrame(race_data['x']['data']).T
        race_df.columns = headers
        race_df = race_df.set_index('Race/Ethnicity')
        race_df['Cases'] = race_df['Cases'].astype(
            str
        ).str.replace('&lt;', '<')
        race_df['Deaths'] = race_df['Deaths'].astype(
            str
        ).str.replace('&lt;', '<')

        cnt_cases = race_df.loc['Statewide', 'Cases']
        cnt_deaths = race_df.loc['Statewide', 'Deaths']
        cnt_cases_aa = race_df.loc['Black/African American', 'Cases']
        cnt_deaths_aa = race_df.loc['Black/African American', 'Deaths']
        pct_cases_aa = race_df.loc['Black/African American', '% of Cases']
        try:
            pct_deaths_aa = round(100 * int(cnt_deaths_aa) / int(cnt_deaths),
                                  2)
        except ValueError:
            pct_deaths_aa = float('nan')

        return [self._make_series(
            date=date,
            cases=cnt_cases,
            deaths=cnt_deaths,
            aa_cases=cnt_cases_aa,
            aa_deaths=cnt_deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
