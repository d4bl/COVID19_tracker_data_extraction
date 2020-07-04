import datetime
import logging

from pytz import timezone
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import table_to_dataframe
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int
from covid19_scrapers.utils.selenium import url_to_soup_with_selenium


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
        now = datetime.datetime.now(timezone('US/Eastern'))
        return (now.date() - datetime.timedelta(days=1)
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

    def get_race_and_ethnicity_table(self, soup):
        race_and_ethnicity_text = soup.find(
            'strong', text='By Race and Ethnicity')
        df = table_to_dataframe(race_and_ethnicity_text.find_next('table'))
        df['Race/Ethnicity'] = df['Race/Ethnicity'].str.strip()
        # df['Cases'] should have been converted by utils._maybe_convert
        df['Deaths'] = df['Deaths'].str.extract(
            r'\(([0-9,]+)\)', expand=False).apply(raw_string_to_int)
        return df.set_index('Race/Ethnicity')

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
        race_df = self.get_race_and_ethnicity_table(soup)
        known_cases = cases - race_df.loc['Data not available', 'Cases']
        known_deaths = cases - race_df.loc['Data not available', 'Deaths']
        aa_cases = race_df.loc['African-American (NH)', 'Cases']
        aa_deaths = race_df.loc['African-American (NH)', 'Deaths']
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
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
