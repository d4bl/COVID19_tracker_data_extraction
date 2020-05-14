from io import StringIO
import logging
import numpy as np
import os
import pandas as pd
import traceback as tb

from covid19_scrapers.dir_context import dir_context


ERROR = 'An error occurred.'
SUCCESS = 'Success!'
_logger = logging.getLogger(__name__)


class ScraperBase(object):
    """Base class for the scrapers providing common scraper functionality
    such as error handling.
    """

    def __init__(self, *, home_dir, **kwargs):
        """home_dir: required, Pathlike to a directory that will contain any
        working files this scraper writes.  This includes any
        downloaded or cached files. The directory will be created if
        it does not exist.
        """
        self.home_dir = home_dir
        os.makedirs(str(home_dir), exist_ok=True)

    def name(self):
        """Returns the human-readable name of the location for which this
        scraper extracts data. Unless overridden, this defaults to the
        subclass's name.
        """
        return self.__class__.__name__

    def run(self, *, validation=False):
        """Invoke the subclass's _scrape method and return the result or an
        error row. _scrape must return a list (possibly empty) of
        pandas Series objects, or a DataFrame.

        In case of exceptions, _handle_error is used to produce an error row.
        """
        # dir_context is a helper to change to the home_dir and back.
        with dir_context(self.home_dir):
            try:
                _logger.info(f'Scraping {self.name()}')
                rows = self._scrape(validation)
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
        """Returns a pandas.Series with the common scraping fields set to the
        specified values.
        """
        # Warn if there were errors.
        if status != SUCCESS:
            _logger.warning(status)

        return pd.Series({
            'Location': self.name(),
            'Date Published': date,
            'Total Cases': cases,
            'Total Deaths': deaths,
            'Count Cases Black/AA': aa_cases,
            'Count Deaths Black/AA': aa_deaths,
            'Pct Cases Black/AA': pct_aa_cases,
            'Pct Deaths Black/AA': pct_aa_deaths,
            'Status code': status,
        })

    def _handle_error(self, e):
        """Returns a row indicating that an exception occurred, and log a
        traceback.

        You can override this in subclasses if you need more
        specialized error handling.
        """
        _logger.exception(e)
        return [self._make_series(status=self._format_error(e))]

    def _format_error(self, e):
        """Generate a descriptive string for an exception.

        _handle_error uses this to set the 'Status code' field.

        You can override this in subclasses if you need more
        specialized error formatting.
        """
        return f'{ERROR} ... {repr(e)}'
