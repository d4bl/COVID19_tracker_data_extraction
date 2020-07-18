from datetime import date
from pathlib import Path

import mock
import pandas as pd
from bs4 import BeautifulSoup

from covid19_scrapers.test.states.data import loader
from covid19_scrapers.utils.testing import FakeCensusApi
from covid19_scrapers.webdriver.runner import WebdriverResults


def run_scraper_and_assert(*, scraper_cls, assertions):
    scraper = scraper_cls(home_dir=Path('test'), census_api=FakeCensusApi())
    results = scraper._scrape()
    assert len(results) == 1
    result = results[0]
    for key, value in assertions.items():
        assert result[key] == value, f'Failed on field: {key}, value: {value}'
    return results


def mock_url_to_soup(template):
    def _mock_url_to_soup(*args, **kwargs):
        html = loader.get_template(template).render()
        return BeautifulSoup(html, 'lxml')
    return _mock_url_to_soup


def make_query_geoservice_data(data):
    return (date.today(), pd.DataFrame.from_dict(data))


def mocked_webdriver_runner(template=None, as_soup=True):
    mocked_driver = MockWebdriverRunner(
        template=template,
        as_soup=as_soup)
    return mock.MagicMock(return_value=mocked_driver)


class MockWebdriverRunner(object):
    def __init__(self, template=None, as_soup=True):
        self.template = self._handle_template(template, as_soup)

    def _handle_template(self, template, as_soup):
        if not template:
            return None
        rendered_template = loader.get_template(template).render()
        if as_soup:
            rendered_template = BeautifulSoup(rendered_template, 'lxml')
        return rendered_template

    def run(self, *args, **kwargs):
        return WebdriverResults(
            x_session_id=None,
            requests=None,
            page_source=self.template)
