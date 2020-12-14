import logging
import numpy as np
import os
import pandas as pd

from covid19_scrapers.dir_context import dir_context
from covid19_scrapers.census import get_aa_pop_stats


ERROR = 'An error occurred.'
SUCCESS = 'Success!'
_logger = logging.getLogger(__name__)


class ScraperBase(object):
    """Base class for the scrapers providing common scraper functionality
    such as error handling.
    """

    def __init__(self, *, home_dir, census_api, **kwargs):
        """Arguments:
          home_dir: Pathlike to a directory that will contain any
            working files this scraper writes.  This includes any
            downloaded or cached files. The directory will be created
            if it does not exist.

          census_api: instance of
            covid19_scrapers.census_api.CensusApi.
          start_date: start date for scraper output, or None.
          end_date: end date for scraper output.

        """
        self.home_dir = home_dir
        self.census_api = census_api
        os.makedirs(str(home_dir), exist_ok=True)

    def name(self):
        """Returns the human-readable name of the location for which this
        scraper extracts data. Unless overridden, this defaults to the
        subclass's name.
        """
        return self.__class__.__name__

    def run(self, start_date, end_date, **kwargs):
        """Invoke the subclass's _scrape method and return the result or an
        error row. _scrape must return a list (possibly empty) of
        pandas Series objects, or a DataFrame.

        In case of exceptions, _handle_error is used to produce an error row.
        """
        # dir_context is a helper to change to the home_dir and back.
        with dir_context(self.home_dir):
            try:
                _logger.info(f'Scraping {self.name()}')
                rows = self._scrape(start_date=start_date, end_date=end_date,
                                    **kwargs)
            except Exception as e:
                rows = self._handle_error(e)
        ret = pd.DataFrame(rows)

        # Get location demographics if there were not errors:
        aa_pop = None
        pct_aa_pop = None
        good_rows = (ret['Status code'] == SUCCESS)
        if good_rows.any():
            aa_pop, _, pct_aa_pop = self._get_aa_pop_stats()
            ret.loc[good_rows, 'Black/AA Population'] = aa_pop
            ret.loc[good_rows, 'Pct Black/AA Population'] = pct_aa_pop

        # Warn if there were errors.
        for (_, row) in ret[~good_rows].iterrows():
            if pd.notnull(row['Date Published']):
                _logger.warning(f'{row["Date Published"]}: {row["Status code"]}')
            else:
                _logger.warning(row['Status code'])
        return ret

    @classmethod
    def is_beta(cls):
        return getattr(cls, 'BETA_SCRAPER', False)

    # TODO: support multi-geography scrapers (state and county, eg)
    def _get_aa_pop_stats(self):
        """This default will retrieve AA population stats for state
        scrapers. For city/county scrapers, you will need to override
        it.  See LA/SD for examples.

        Returns triple of (aa_pop, total_pop, aa_pct).

        """
        try:
            return get_aa_pop_stats(self.census_api, self.name())
        except Exception:
            return (None, None, None)

    def _make_series(
            self, *,
            location='',
            date='',
            cases=np.nan, deaths=np.nan,
            aa_cases=np.nan, aa_deaths=np.nan,
            pct_aa_cases=np.nan,
            pct_aa_deaths=np.nan,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=np.nan,
            known_race_deaths=np.nan,
            status=SUCCESS):
        """Returns a pandas.Series with the common scraping fields set to the
        specified values.

        """
        return pd.Series({
            'Location': location or self.name(),
            'Date Published': date,
            'Total Cases': cases,
            'Total Deaths': deaths,
            'Count Cases Black/AA': aa_cases,
            'Count Deaths Black/AA': aa_deaths,
            'Pct Cases Black/AA': pct_aa_cases,
            'Pct Deaths Black/AA': pct_aa_deaths,
            'Pct Includes Unknown Race': pct_includes_unknown_race,
            'Pct Includes Hispanic Black': pct_includes_hispanic_black,
            'Count Cases Known Race': known_race_cases,
            'Count Deaths Known Race': known_race_deaths,
            'Status code': status,
        })

    def _make_dataframe(self, *, location=None, cases, deaths,
                        aa_cases, aa_deaths, pct_aa_cases,
                        pct_aa_deaths, pct_includes_unknown_race,
                        pct_includes_hispanic_black, known_race_cases,
                        known_race_deaths, status):
        """Returns a pandas.DataFrame with the common scraping fields set to
        the specified pandas.Series values.  The series must all be
        indexed with pandas.DatetimeIndexes.

        """
        ret = pd.DataFrame({
            'Location': location or self.name(),
            'Total Cases': cases,
            'Total Deaths': deaths,
            'Count Cases Black/AA': aa_cases,
            'Count Deaths Black/AA': aa_deaths,
            'Pct Cases Black/AA': pct_aa_cases,
            'Pct Deaths Black/AA': pct_aa_deaths,
            'Pct Includes Unknown Race': pct_includes_unknown_race,
            'Pct Includes Hispanic Black': pct_includes_hispanic_black,
            'Count Cases Known Race': known_race_cases,
            'Count Deaths Known Race': known_race_deaths,
            'Status code': status,
        })
        ret.index.name = 'Date Published'
        return ret.reset_index()

    def _handle_error(self, e, date=None):
        """Returns a row indicating that an exception occurred, and log a
        traceback.

        You can override this in subclasses if you need more
        specialized error handling.
        """
        _logger.exception(e)
        return [self._make_series(date=date, status=self._format_error(e))]

    def _format_error(self, e):
        """Generate a descriptive string for an exception.

        _handle_error uses this to set the 'Status code' field.

        You can override this in subclasses if you need more
        specialized error formatting.
        """
        return f'{ERROR} ... {repr(e)}'
