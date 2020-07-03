from functools import reduce
import logging
import pandas as pd

from covid19_scrapers.web_cache import WebCache
from covid19_scrapers.utils import UTILS_WEB_CACHE


_logger = logging.getLogger(__name__)


class Registry(object):
    """A registry for scrapers.

    This provides scaffolding code for registering scrapers based on
    their name(), and running one, several, or all registered
    scrapers, and collecting their results.
    """

    def __init__(self, *, home_dir, enable_beta_scrapers=False, **kwargs):
        """Returns a Registry instance with all the per-state scrapers
        registered.

        Keyword arguments:
          home_dir: Path-like for the cache/download directory root.
          enable_beta_scrapers: optional, a bool indicating whether to
            include scrapers with the BETA_SCRAPER class variable set.

        """
        self.enable_beta_scrapers = enable_beta_scrapers
        self.home_dir = home_dir
        self.web_cache = WebCache(str(home_dir / 'web_cache.db'))
        self._scrapers = {}

    def register_scraper(self, instance):
        """Add a scraper to this registry's dictionary using its
        class name.
        """
        name = instance.__class__.__name__
        _logger.debug(f'Registering scraper: {name}: {instance.name()}')
        self._scrapers[name] = instance

    def scraper_names(self):
        """Return an interable of the names of all the registered scrapers."""
        return self._scrapers.keys()

    def scrapers(self):
        """Return an interable of all the registered scrapers."""
        return self._scrapers.values()

    def run_scraper(self, name, **kwargs):
        """Return the results of running the specified scraper, or None if no
        such scraper is registered.
        """
        scraper = self._scrapers.get(name)
        if scraper:
            with UTILS_WEB_CACHE(instance=self.web_cache):
                if scraper.is_beta() and not self.enable_beta_scrapers:
                    _logger.warn(f'Running beta scraper: {scraper.name()}')
                return scraper.run(**kwargs)

    def run_scrapers(self, names, **kwargs):
        """Return the results of running the specified scrapers, or an empty
        Dataframe if no such scrapers are registered.
        """
        ret = []
        for name in names:
            df = self.run_scraper(name, **kwargs)
            if df is not None:
                ret.append(df)
        if ret:
            # Append the DFs in the list together, going from left
            # to right.
            return reduce((lambda df1, df2: df1.append(df2)), ret)
        return pd.DataFrame()

    def run_all_scrapers(self, **kwargs):
        """Return the results of running all registered scrapers, or an empty
        Dataframe if no scrapers are registered.
        """
        ret = []
        with UTILS_WEB_CACHE(instance=self.web_cache):
            for scraper in self._scrapers.values():
                if scraper.is_beta() and not self.enable_beta_scrapers:
                    _logger.debug(f'Skipping beta scraper: {scraper.name()}')
                    continue
                ret.append(scraper.run(**kwargs))
        if ret:
            # Append the DFs in the list together, going from left
            # to right.
            return reduce((lambda df1, df2: df1.append(df2)), ret)
        return pd.DataFrame()
