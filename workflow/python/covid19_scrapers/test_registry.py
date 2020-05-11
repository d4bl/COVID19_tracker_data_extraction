from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase

class MockScraperOneSeries(ScraperBase):
    def _scrape(self, unused1, unused2):
        return [self._make_series()]

class MockScraperTwoSeries(ScraperBase):
    def _scrape(self, unused1, unused2):
        return [self._make_series(), self._make_series()]
        
class TestRegistry(object):
    def __init__(self):
        self.registry=None

    def setup(self):
        self.registry = Registry()

    def test_empty_registry(self):
        df = self.registry.run_all_scrapers()
        assert df.shape[0] == 0

    def test_oneseries_registry(self):
        self.registry.register_scraper(MockScraperOneSeries())
        df = self.registry.run_scraper('MockScraperOneSeries')
        assert df.shape[0] == 1

        df = self.registry.run_scrapers(['MockScraperOneSeries'])
        assert df.shape[0] == 1

        df = self.registry.run_all_scrapers()
        assert df.shape[0] == 1

    def test_twoseries_registry(self):
        self.registry.register_scraper(MockScraperTwoSeries())
        df = self.registry.run_scraper('MockScraperTwoSeries')
        assert df.shape[0] == 2

        df = self.registry.run_scrapers(['MockScraperTwoSeries'])
        assert df.shape[0] == 2

        df = self.registry.run_all_scrapers()
        assert df.shape[0] == 2

    def test_multi_registry(self):
        self.registry.register_scraper(MockScraperOneSeries())
        self.registry.register_scraper(MockScraperTwoSeries())

        df = self.registry.run_scraper('MockScraperOneSeries')
        assert df.shape[0] == 1

        df = self.registry.run_scrapers(['MockScraperOneSeries'])
        assert df.shape[0] == 1

        df = self.registry.run_scraper('MockScraperTwoSeries')
        assert df.shape[0] == 2

        df = self.registry.run_scrapers(['MockScraperTwoSeries'])
        assert df.shape[0] == 2

        df = self.registry.run_scrapers(['MockScraperOneSeries',
                                         'MockScraperTwoSeries'])
        assert df.shape[0] == 3

        df = self.registry.run_all_scrapers()
        assert df.shape[0] == 3

    
