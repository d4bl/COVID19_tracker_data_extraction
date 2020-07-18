from datetime import date
import mock

from covid19_scrapers.states.arkansas import Arkansas
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.arkansas.query_geoservice',
            mock.MagicMock(return_value=util.make_query_geoservice_data(json_file='arkansas.json')))
def test_arkansas():
    util.run_scraper_and_assert(
        scraper_cls=Arkansas,
        assertions={
            'Date Published': date.today(),
            'Total Cases': 31000,
            'Total Deaths': 300,
            'Count Cases Black/AA': 6000,
            'Count Deaths Black/AA': 90,
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': True,
            'Pct Cases Black/AA': to_percentage(6000, 27000),
            'Pct Deaths Black/AA': to_percentage(90, 297),
            'Count Cases Known Race': 27000,
            'Count Deaths Known Race': 297
        })
