from datetime import date
from pathlib import Path

import mock
import pandas as pd
from bs4 import BeautifulSoup

from covid19_scrapers.test.states.data import loader
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.testing import FakeCensusApi
from covid19_scrapers.webdriver.runner import WebdriverResults


def run_scraper_and_assert(*, scraper_cls, assertions):
    scraper = scraper_cls(home_dir=Path('test'), census_api=FakeCensusApi())
    results = scraper._scrape(start_date=None, end_date=pd.Timestamp.today())
    assert len(results) == 1
    result = results[0]
    for key, value in assertions.items():
        assert result[key] == value, f'Failed on field: {key}. {result[key]} != {value}'
    return results


def mock_aa_pop_stats(aa_pop=1000, total_pop=50000):
    return mock.MagicMock(return_value=(aa_pop, total_pop, to_percentage(aa_pop, total_pop)))


def mock_url_to_soup(template):
    def _mock_url_to_soup(*args, **kwargs):
        html = loader.get_template(template).render()
        return BeautifulSoup(html, 'lxml')
    return _mock_url_to_soup


def make_query_geoservice_data(*, data=None, json_file=None):
    assert bool(data) != bool(json_file), 'a dictionary or json_file (in data.json) needs to be passed in, but not both.'
    if not data:
        data = loader.get_json(json_file)
    return (date.today(), pd.DataFrame.from_dict(data))


def mock_read_csv_dataframe(csv_file, **kwargs):
    return loader.get_csv(csv_file, **kwargs)


def mocked_webdriver_runner(template=None, as_soup=True, requests=None):
    mocked_driver = MockWebdriverRunner(
        template=template,
        as_soup=as_soup,
        requests=requests)
    return mock.MagicMock(return_value=mocked_driver)


class MockWebdriverRunner(object):
    def __init__(self, template=None, as_soup=True, requests=None):
        self.template = self._handle_template(template, as_soup)
        self.requests = requests

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
            requests=self.requests,
            page_source=self.template)


def mock_response(*, json_file=None, blob_file=None):
    assert bool(json_file) != bool(blob_file), 'Exactly one of `json_file` or `blob_file` should be passed in'
    text = None
    if json_file:
        text = loader.get_json(json_file)
    if blob_file:
        text = loader.get_blob(blob_file)
    return MockResponse(text=text)


def magic_mock_response(**kwargs):
    """This is a wrapper for mock_response.

    Just wraps mock_response in a magic mock if needed for ease.
    """
    return mock.MagicMock(return_value=mock_response(**kwargs))


class MockResponse(object):
    def __init__(self, text):
        self.text = text

    @property
    def body(self):
        return str.encode(self.text)


class MockSeleniumWireRequest(object):
    def __init__(self, response_body):
        self.response = MockResponse(response_body)
