import mock

from covid19_scrapers.states.maryland import Maryland
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.maryland.WebdriverRunner',
            util.mocked_webdriver_runner(template='maryland.jinja2'))
def test_maryland():
    util.run_scraper_and_assert(
        scraper_cls=Maryland,
        assertions={
            'Total Cases': 75000,
            'Total Deaths': 3000,
            'Count Cases Black/AA': 20000,
            'Count Deaths Black/AA': 1000,
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(20000, 63000),
            'Pct Deaths Black/AA': to_percentage(1000, 2990),
            'Count Cases Known Race': 63000,
            'Count Deaths Known Race': 2990
        })
