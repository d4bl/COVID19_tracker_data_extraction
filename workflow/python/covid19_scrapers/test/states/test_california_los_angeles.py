from datetime import datetime
import mock

from covid19_scrapers.states.california_los_angeles import CaliforniaLosAngeles
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.california_los_angeles.get_cached_url',
            util.magic_mock_response(blob_file='california_los_angeles.txt'))
@mock.patch('covid19_scrapers.states.california_los_angeles.url_to_soup',
            util.mock_url_to_soup(template='california_los_angeles.jinja2'))
@mock.patch('covid19_scrapers.states.california_los_angeles.get_aa_pop_stats',
            util.mock_aa_pop_stats())
def test_california_los_angeles():
    util.run_scraper_and_assert(
        scraper_cls=CaliforniaLosAngeles,
        assertions={
            'Date Published': datetime(2020, 7, 16).date(),
            'Total Cases': 150000,
            'Total Deaths': 4000,
            'Count Cases Black/AA': 4000,
            'Count Deaths Black/AA': 400,
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(4000, 78500),
            'Pct Deaths Black/AA': to_percentage(400, 3650),
            'Count Cases Known Race': 78500,
            'Count Deaths Known Race': 3650
        }
    )
