import logging
import numpy as np
import pandas as pd


SUCCESS = 'Success!'
_logger = logging.getLogger('covid19_scrapers')


class ScraperBase(object):
    """Base class for the scrapers providing common functionality such as
    error handling.
    """
    
    def name(self):
        return self.__class__.__name__
    
    def run(self, *, validation=False, home_dir=None):
        """Invoke the subclass's scrape method and return the result or an
        error row. _scrape must return a list, possibly empty, of pandas Series objects, or a DataFrame.
        """
        try:
            _logger.info(f'Scraping {self.name()}')
            rows = self._scrape(validation, home_dir)
        except Exception as e:
            rows = self._handle_error(e)
        return pd.DataFrame(rows)

    def _make_series(
            self, *,
            date='',
            cases=np.nan, deaths=np.nan,
            aa_cases=np.nan, aa_deaths=np.nan,
            pct_aa_cases=np.nan,
            pct_aa_deaths=np.nan,
            status=SUCCESS):
        """Returns a pandas.Series with common scraping fields set to the
        specified values.
        """
        if status != SUCCESS:
            _logger.warning(status)
        
        return pd.Series({
            'Location': self.name(),
            'Date Published': date,
            'Total Cases': cases,
            'Total Deaths': deaths,
            'Black/AA Cases': aa_cases,
            'Black/AA Deaths': aa_deaths,
            'Pct Cases Black/AA': pct_aa_cases,
            'Pct Deaths Black/AA': pct_aa_deaths,
            'Status code': status,
        })
        
    def _handle_error(self, e):
        """General error handler to return a failure row.
        Override in subclasses for specialized error handling.
        """
        return [self._make_series(status=self._format_error(e))]
        
    def _format_error(self, e):
        return f'ERROR: {repr(e)}'
