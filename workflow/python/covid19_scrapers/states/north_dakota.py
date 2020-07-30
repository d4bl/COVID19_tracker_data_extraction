from datetime import datetime
import re

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import url_to_soup
from covid19_scrapers.utils.parse import raw_string_to_int


class NorthDakota(ScraperBase):
    URL = 'https://www.health.nd.gov/diseases-conditions/coronavirus/north-dakota-coronavirus-cases'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _parse(self, soup, text):
        parsed_text = soup.find('p', text=text)
        parsed_num = parsed_text.find_previous('h2')
        return raw_string_to_int(parsed_num.text)

    def get_cases(self, soup):
        return self._parse(soup, text='Positive Cases')

    def get_deaths(self, soup):
        return self._parse(soup, text='Deaths')

    def get_date(self, soup):
        last_updated = soup.find('p', text=re.compile(r'Last updated'))
        return datetime.strptime(last_updated.text, 'Last updated: %m/%d/%Y').date()

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.URL)
        date = self.get_date(soup)
        cases = self.get_cases(soup)
        deaths = self.get_deaths(soup)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
        )]
