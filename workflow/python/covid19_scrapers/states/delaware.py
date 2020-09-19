import logging
import re
from datetime import datetime

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import table_to_dataframe
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps

_logger = logging.getLogger(__name__)


class Delaware(ScraperBase):
    DATA_URL = 'https://myhealthycommunity.dhss.delaware.gov/locations/state'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_last_updated_date(self, soup):
        # There are many places on the page that show the last updated
        # date.  Choosing the updated date from the dashboard that
        # will be parsed for a more probable accurate date.
        total_cases_by_race = soup.find(
            'div', {'id': 'total-cases-by-race-ethnicity'})
        updated_card = total_cases_by_race.find_previous(
            'div', {'class': 'c-dashboard-card__updated'})

        # There are two span elements under the updated_card. One is a
        # text that simply says "Updated", the second is the date.
        spans = updated_card.find_all('span')
        assert len(spans) == 2, 'Unable to parse date for Delaware'
        return datetime.strptime(spans[1].text.strip(), '%m/%d/%Y').date()

    def get_total_cases(self, soup):
        # There are multiple places on the page that display the
        # `total cases`, all of which have the same value.  This just
        # randomly chooses one and parses it.
        total_cases_text = soup.find('span', text='Total Positive Cases')
        total_cases_value = total_cases_text.find_next(
            'span',
            class_='c-summary-metric__value')
        return raw_string_to_int(total_cases_value.text)

    def get_total_deaths(self, soup):
        total_deaths_text = soup.find('span', text='Total Deaths')
        total_deaths_value = total_deaths_text.find_next(
            'span', class_='c-summary-metric__value')
        return raw_string_to_int(total_deaths_value.text)

    def _parse_aa_df(self, soup, text):
        title = soup.find('h4', text=text)
        parsed = title.find_next('table')
        df = table_to_dataframe(parsed)
        assert 'Race/Ethnicity' in df.columns
        assert 'State of Delaware' in df.columns, 'Unable to parse Total AA cases for Delaware'
        df['State of Delaware'] = df['State of Delaware'].apply(lambda num: raw_string_to_int(num.split('\n')[0]))
        return df.set_index('Race/Ethnicity')

    def get_race_cases_df(self, soup):
        return self._parse_aa_df(soup, re.compile(r'total cases by race', re.I))

    def get_race_deaths_df(self, soup):
        return self._parse_aa_df(soup, re.compile(r'total deaths by race', re.I))

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.DATA_URL)
            .find_element_by_xpath("//a[@data-chart-id='count-charts']")
            .click_on_last_element_found()
            .wait_for_presence_of_elements((By.XPATH, "//*[contains(text(), 'Total Cases by Race/Ethnicity & County')]"))
            .get_page_source()
        )
        soup = results.page_source

        date = self.get_last_updated_date(soup)
        _logger.info(f'Processing data for {date}')

        cases = self.get_total_cases(soup)
        deaths = self.get_total_deaths(soup)

        cases_df = self.get_race_cases_df(soup)
        aa_cases = cases_df.loc['Non-Hispanic Black']['State of Delaware']
        try:
            unknown_race_cases = cases_df.loc['Unknown']['State of Delaware']
        except KeyError:
            unknown_race_cases = 0
        known_race_cases = cases - unknown_race_cases

        deaths_df = self.get_race_deaths_df(soup)
        aa_deaths = deaths_df.loc['Non-Hispanic Black']['State of Delaware']
        try:
            unknown_race_deaths = deaths_df.loc['Unknown']['State of Delaware']
        except KeyError:
            unknown_race_deaths = 0
        known_race_deaths = deaths - unknown_race_deaths

        pct_aa_cases = to_percentage(aa_cases, known_race_cases)
        pct_aa_deaths = to_percentage(aa_deaths, known_race_deaths)

        return [self._make_series(
            date=self.get_last_updated_date(soup),
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_race_cases,
            known_race_deaths=known_race_deaths
        )]
