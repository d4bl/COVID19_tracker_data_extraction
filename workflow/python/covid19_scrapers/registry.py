from functools import reduce
import logging
import os
import pandas as pd

from covid19_scrapers.dir_context import dir_context

_logger = logging.getLogger('covid19_scrapers')

class Registry(object):
    """A registry for scrapers.
    """

    def __init__(self, *, home_dir, **kwargs):
        self.home_dir = home_dir
        os.makedirs(str(self.home_dir), exist_ok=True)
        self._scrapers = {}

    def register_scraper(self, instance):
        _logger.debug(f'Registering scraper: {instance.name()}')
        self._scrapers[instance.name()] = instance
        
    def scraper_names(self):
        return self._scrapers.keys()

    def scrapers(self):
        return self._scrapers.values()
                
    def run_scraper(self, name, **kwargs):
        scraper = self._scrapers.get(name)
        if scraper:
            return scraper.run(**kwargs)
        
    def run_scrapers(self, names, **kwargs):
        ret = []
        for name in names:
            df = self.run_scraper(name, **kwargs)
            if df is not None:
                ret.append(df)
        if ret:
            return reduce((lambda df1, df2: df1.append(df2)), ret)
        return pd.DataFrame()

    def run_all_scrapers(self, **kwargs):
        ret = []
        for scraper in self._scrapers.values():
            ret.append(scraper.run(**kwargs))
        if ret:
            return reduce((lambda df1, df2: df1.append(df2)), ret)
        return pd.DataFrame()
