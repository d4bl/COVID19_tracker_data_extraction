from datetime import datetime
import mock

from covid19_scrapers.states.connecticut import Connecticut
from covid19_scrapers.test.states import util
from covid19_scrapers.test.states.data import loader
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.connecticut.get_json',
            side_effect=[
                loader.get_json('connecticut_cases.json'),
                loader.get_json('connecticut_race.json')])
def test_connecticut(mock_get_json):
    util.run_scraper_and_assert(
        scraper_cls=Connecticut,
        assertions={
            'Date Published': datetime(2020, 7, 16).date(),
            'Total Cases': 30000,
            'Total Deaths': 3000,
            'Count Cases Black/AA': 6000,
            'Count Deaths Black/AA': 600,
            'Pct Includes Unknown Race': True,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(6000, 30000),
            'Pct Deaths Black/AA': to_percentage(600, 3000),
        })
