import logging
from datetime import datetime
import re

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import table_to_dataframe, url_to_soup
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int


_logger = logging.getLogger(__name__)


class Delaware(ScraperBase):
    DATA_URL = 'https://myhealthycommunity.dhss.delaware.gov/locations/state/'

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
        total_cases_text = soup.find('span', text='Total Cases')
        total_cases_value = total_cases_text.find_next(
            'span',
            class_='c-summary-metric__value')
        return raw_string_to_int(total_cases_value.text)

    def get_total_deaths(self, soup):
        total_deaths_text = soup.find('span', text='Total deaths')
        total_deaths_value = total_deaths_text.find_previous(
            'div', class_='c-summary-metric__value')
        return raw_string_to_int(total_deaths_value.text)

    def _parse_aa_df(self, soup, text):
        title = soup.find('h4', text=text)
        parsed = title.find_next('table')
        df = table_to_dataframe(parsed)
        assert 'Race/Ethnicity' in df.columns
        return df.set_index('Race/Ethnicity')

    def get_aa_cases(self, soup):
        df = self._parse_aa_df(soup, re.compile(r'cases by race', re.I))
        assert 'State of Delaware' in df.columns, 'Unable to parse Total AA cases for Delaware'
        raw_value = df.loc['Non-Hispanic Black', 'State of Delaware']
        # the raw value consists of total and pct seperated by a \n
        total, _ = raw_value.split('\n')
        return int(total)

    def get_aa_deaths(self, soup):
        df = self._parse_aa_df(soup, re.compile(r'deaths by race', re.I))
        assert 'State of Delaware' in df.columns, 'Unable to parse Total AA deaths for Delaware'
        raw_value = df.loc['Non-Hispanic Black', 'State of Delaware']
        # the raw value consists of total and pct seperated by a \n
        total, _ = raw_value.split('\n')
        return int(total)

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.DATA_URL)

        date = self.get_last_updated_date(soup)
        _logger.info(f'Processing data for {date}')

        cases = self.get_total_cases(soup)
        deaths = self.get_total_deaths(soup)
        aa_cases = self.get_aa_cases(soup)
        aa_deaths = self.get_aa_deaths(soup)
        pct_aa_cases = to_percentage(aa_cases, cases)
        pct_aa_deaths = to_percentage(aa_deaths, deaths)

        return [self._make_series(
            date=self.get_last_updated_date(soup),
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
