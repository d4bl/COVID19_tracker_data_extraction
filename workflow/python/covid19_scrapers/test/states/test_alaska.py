from datetime import date
import mock

from covid19_scrapers.states.alaska import Alaska
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.alabama.query_geoservice',
            mock.MagicMock(return_value=util.make_query_geoservice_data(json_file='alaska.json')))
def test_alaska():
    util.run_scraper_and_assert(
        scraper_cls=Alaska,
        assertions={
            'Date Published': date.today(),
            'Total Cases': 1733,
            'Total Deaths': 17,
            'Count Cases Black/AA': 41,
            'Count Deaths Black/AA': 0,
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': True,
            'Pct Cases Black/AA': to_percentage(41, 1171),
            'Pct Deaths Black/AA': to_percentage(0, 17),
            'Count Cases Known Race': 1171,
            'Count Deaths Known Race': 17
        })
