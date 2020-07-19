
from datetime import datetime
import mock

from covid19_scrapers.states.illinois import Illinois
from covid19_scrapers.test.states import util
from covid19_scrapers.test.states.data import loader
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.illinois.get_json', mock.MagicMock(return_value=loader.get_json('illinois.json')))
def test_illinois():
    util.run_scraper_and_assert(
        scraper_cls=Illinois,
        assertions={
            'Date Published': datetime(2020, 7, 18).date(),
            'Total Cases': 160000,
            'Total Deaths': 7000,
            'Count Cases Black/AA': 27000,
            'Count Deaths Black/AA': 2000,
            'Pct Includes Unknown Race': True,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(27000, 160000),
            'Pct Deaths Black/AA': to_percentage(2000, 7000)
        })
