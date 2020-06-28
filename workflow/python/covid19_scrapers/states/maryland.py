from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import (
    raw_string_to_int, to_percentage, url_to_soup_with_selenium)

from datetime import datetime, timedelta
import logging
from pytz import timezone
import re
from selenium.webdriver.common.by import By

_logger = logging.getLogger(__name__)


class Maryland(ScraperBase):
    DATA_URL = 'https://coronavirus.maryland.gov'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_date(self):
        # HACK: No reliable date could be found. However, the site
        # states that the dataset is updated at 10am everyday.  So
        # check for current US/Eastern time. If it is past 10, use the
        # current date, otherwise use yesterday's date.
        now = datetime.now(timezone('US/Eastern'))
        return (now.date() - timedelta(days=1)
                if now.hour < 10
                else now.date())

    def get_total_cases(self, soup):
        total_cases_text = soup.find(text='Number of confirmed cases : ')
        total_count_string = total_cases_text.next_element
        return raw_string_to_int(total_count_string)

    def get_total_deaths(self, soup):
        total_deaths_text = soup.find(text='Number of confirmed deaths : ')
        death_count_string = total_deaths_text.next_element
        return raw_string_to_int(death_count_string)

    def _get_race_and_ethnicity_table(self, soup):
        race_and_ethnicity_text = soup.find(
            'strong', text='By Race and Ethnicity')
        return race_and_ethnicity_text.find_next('table')

    def get_aa_cases(self, soup):
        table = self._get_race_and_ethnicity_table(soup)
        aa_text = table.find_next('td', text=re.compile('African-American'))
        # Next td is total cases for AA, and after that is total
        # deaths for AA.
        return raw_string_to_int(aa_text.find_next('td').text)

    def get_aa_deaths(self, soup):
        table = self._get_race_and_ethnicity_table(soup)
        aa_text = table.find_next('td', text=re.compile('African-American'))

        total_aa_count = aa_text.find_next('td')

        # Next td is total cases for AA, and after that is total
        # deaths for AA
        return raw_string_to_int(total_aa_count.find_next('td').text)

    def _scrape(self, **kwargs):
        soup = url_to_soup_with_selenium(
            self.DATA_URL,
            wait_conditions=[
                (By.CLASS_NAME, 'markdown-card'),
                (By.CLASS_NAME, 'ember-view')
            ])

        date = self.get_date()
        _logger.info(f'Processing data for {date}')

        cases = self.get_total_cases(soup)
        deaths = self.get_total_deaths(soup)
        aa_cases = self.get_aa_cases(soup)
        aa_deaths = self.get_aa_deaths(soup)
        pct_aa_cases = to_percentage(aa_cases, cases)
        pct_aa_deaths = to_percentage(aa_deaths, deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False
        )]
