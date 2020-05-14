from functools import reduce
import logging
import os
import pandas as pd

from covid19_scrapers.dir_context import dir_context

_logger = logging.getLogger(__name__)

class Registry(object):
    """A registry for scrapers.

    This provides scaffolding code for registering scrapers based on
    their name(), and running one, several, or all registered
    scrapers, and collecting their results.
    """

    def __init__(self, **kwargs):
        self._scrapers = {}

    def register_scraper(self, instance):
        """Add a scraper to this registry's dictionary using its
        name()."""
        _logger.debug(f'Registering scraper: {instance.name()}')
        self._scrapers[instance.name()] = instance
        
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
        for scraper in self._scrapers.values():
            ret.append(scraper.run(**kwargs))
        if ret:
            # Append the DFs in the list together, going from left
            # to right.
            return reduce((lambda df1, df2: df1.append(df2)), ret)
        return pd.DataFrame()
