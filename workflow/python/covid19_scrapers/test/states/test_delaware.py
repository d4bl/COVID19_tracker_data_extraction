from datetime import datetime

import mock

from covid19_scrapers.states.delaware import Delaware
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.delaware.url_to_soup',
            util.mock_url_to_soup(template='delaware.jinja2'))
def test_delaware():
    util.run_scraper_and_assert(
        scraper_cls=Delaware,
        assertions={
            'Date Published': datetime(2020, 7, 16).date(),
            'Total Cases': 13000,
            'Total Deaths': 500,
            'Count Cases Black/AA': 3000,
            'Count Deaths Black/AA': 100,
            'Pct Includes Unknown Race': True,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(3000, 13000),
            'Pct Deaths Black/AA': to_percentage(100, 500)
        })
