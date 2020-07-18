from datetime import date

import mock

from covid19_scrapers.states.wisconsin import Wisconsin
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.wisconsin.query_geoservice',
            mock.MagicMock(return_value=util.make_query_geoservice_data(json_file='wisconsin.json')))
def test_wisconsin():
    util.run_scraper_and_assert(
        scraper_cls=Wisconsin,
        assertions={
            'Date Published': date.today(),
            'Total Cases': 40000,
            'Total Deaths': 800,
            'Count Cases Black/AA': 6600,
            'Count Deaths Black/AA': 200,
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': True,
            'Pct Cases Black/AA': to_percentage(6600, 40000 - 3800),
            'Pct Deaths Black/AA': to_percentage(200, 800 - 10),
            'Count Cases Known Race': 40000 - 3800,
            'Count Deaths Known Race': 800 - 10
        })
