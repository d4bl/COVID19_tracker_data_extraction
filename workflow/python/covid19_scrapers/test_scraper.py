from datetime import date

from pathlib import Path

from covid19_scrapers.scraper import ERROR, SUCCESS, ScraperBase
from covid19_scrapers.utils.testing import FakeCensusApi


CENSUS_API = FakeCensusApi()
DATES = {
    'start_date': None,
    'end_date': date.today()
}


def test_empty_scraper():
    class EmptyScraper(ScraperBase):
        def __init__(self):
            super().__init__(home_dir=Path('test'),
                             census_api=CENSUS_API)

        def _scrape(self, start_date, end_date):
            return []
    scraper = EmptyScraper()
    assert scraper.name() == 'EmptyScraper'
    assert scraper.run(**DATES).empty


def test_one_row_scraper():
    class OneRowScraper(ScraperBase):
        def __init__(self):
            super().__init__(home_dir=Path('test'),
                             census_api=CENSUS_API)

        def _scrape(self, start_date, end_date):
            return [self._make_series()]
    scraper = OneRowScraper()
    assert scraper.name() == 'OneRowScraper'
    df = scraper.run(**DATES)
    assert df.shape[0] == 1
    assert df.loc[0, 'Status code'] == SUCCESS


def test_throwing_scraper():
    class ThrowingScraper(ScraperBase):
        def __init__(self):
            super().__init__(home_dir=Path('test'),
                             census_api=CENSUS_API)

        def _scrape(self, start_date, end_date):
            raise ValueError('error')
    scraper = ThrowingScraper()
    assert scraper.name() == 'ThrowingScraper'
    df = scraper.run(**DATES)
    assert df.shape[0] == 1
    assert df.loc[0, 'Status code'] == f"{ERROR} ... ValueError('error')"


def test_throwing_custom_handler_scraper():
    class ThrowingCustomHandlerScraper(ScraperBase):
        def __init__(self):
            super().__init__(home_dir=Path('test'),
                             census_api=CENSUS_API)

        def _scrape(self, start_date, end_date):
            raise ValueError('error')

        def _handle_error(self, e):
            return [self._make_series(status='CUSTOM TEXT')]
    scraper = ThrowingCustomHandlerScraper()
    assert scraper.name() == 'ThrowingCustomHandlerScraper'
    df = scraper.run(**DATES)
    assert df.shape[0] == 1
    assert df.loc[0, 'Status code'] == 'CUSTOM TEXT'


def test_throwing_custom_format_scraper():
    class ThrowingCustomFormatScraper(ScraperBase):
        def __init__(self):
            super().__init__(home_dir=Path('test'),
                             census_api=CENSUS_API)

        def _scrape(self, start_date, end_date):
            raise ValueError('error')

        def _format_error(self, e):
            return 'CUSTOM TEXT'
    scraper = ThrowingCustomFormatScraper()
    assert scraper.name() == 'ThrowingCustomFormatScraper'
    df = scraper.run(**DATES)
    assert df.shape[0] == 1
    assert df.loc[0, 'Status code'] == 'CUSTOM TEXT'
