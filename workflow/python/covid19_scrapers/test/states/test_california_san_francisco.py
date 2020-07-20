from datetime import datetime
import mock

from covid19_scrapers.states.california_san_francisco import CaliforniaSanFrancisco
from covid19_scrapers.test.states import util
from covid19_scrapers.test.states.data import loader
from covid19_scrapers.utils.misc import to_percentage


def test_california_sf():
    mocked_requests = {
        'date': util.MockSeleniumWireRequest(response_body=loader.get_blob('california_san_francisco_date.txt')),
        'cases_by_race': util.MockSeleniumWireRequest(
            response_body=loader.get_blob('california_san_francisco_cases.txt')),
        'deaths_by_race': util.MockSeleniumWireRequest(
            response_body=loader.get_blob('california_san_francisco_deaths.txt'))
    }

    mocked_webdriver = util.mocked_webdriver_runner(requests=mocked_requests)

    with mock.patch('covid19_scrapers.states.california_san_francisco.WebdriverRunner', mocked_webdriver):
        util.run_scraper_and_assert(
            scraper_cls=CaliforniaSanFrancisco,
            assertions={
                'Date Published': datetime(2020, 7, 17).date(),
                'Total Cases': 5100,
                'Total Deaths': 130,
                'Count Cases Black/AA': 300,
                'Count Deaths Black/AA': 5,
                'Pct Includes Unknown Race': False,
                'Pct Includes Hispanic Black': False,
                'Pct Cases Black/AA': to_percentage(300, 4400),
                'Pct Deaths Black/AA': to_percentage(5, 30),
                'Count Cases Known Race': 4400,
                'Count Deaths Known Race': 30
            })
