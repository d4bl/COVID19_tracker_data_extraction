import logging

import pandas as pd

from covid19_scrapers.scraper import ScraperBase, SUCCESS
from covid19_scrapers.utils.http import get_content_as_file
from covid19_scrapers.utils.misc import slice_dataframe, to_percentage

_logger = logging.getLogger(__name__)


class Connecticut(ScraperBase):
    """CT makes their demographic breakdowns available as JSON at these
    URLs as stated in the website
    https://portal.ct.gov/Coronavirus/COVID-19-Data-Tracker

    Totals: https://data.ct.gov/resource/rf3k-f8fg.json
    Race/Ethnicity: https://data.ct.gov/resource/7rne-efic.json

    The above website also provides a link to download a daily pdf
    that has data embedded which is much harder to parse and needs OCR
    scraping as well since the race/ethnicity data is embedded as
    images. There is code which did this parsing from PDF here,
    https://github.com/d4bl/COVID19_tracker_data_extraction/pull/41/commits/bea25c323b52eb01c13db4f5bed1a578b9fa7937
    If at all if its useful to go back to this at some point for any
    reason.

    """

    CASE_DATA_URL = 'https://data.ct.gov/resource/rf3k-f8fg.json'
    RACE_DATA_URL = 'https://data.ct.gov/resource/7rne-efic.json'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, start_date, end_date, **kwargs):
        _logger.debug('Get case totals data')
        totals_df = pd.read_json(
            get_content_as_file(self.CASE_DATA_URL),
            convert_dates=['date'],
            orient='records',
            dtype={'confirmedcases': float, 'confirmeddeaths': float}
        ).set_index(
            'date'
        ).sort_index(
            ascending=False
        )
        totals_df = slice_dataframe(totals_df, start_date, end_date)
        report_dates = totals_df.index
        if len(report_dates) > 1:
            _logger.info('Processing data for '
                         f'{report_dates.min()} - {report_dates.max()}')
        elif len(report_dates) == 1:
            _logger.info(f'Processing data for {report_dates.min()}')
        else:
            _logger.warn('No summary data')
            return
        total_cases = totals_df['confirmedcases'].dropna().astype(int)
        total_deaths = totals_df['confirmeddeaths'].dropna().astype(int)

        _logger.debug('Get race data')
        race_df = pd.read_json(
            get_content_as_file(self.RACE_DATA_URL),
            convert_dates=['dateupdated'],
            orient='records',
            dtype={'case_tot': float, 'deaths': float}
        ).set_index(
            'dateupdated'
        )
        race_df = slice_dataframe(
            race_df, start_date, end_date
        ).reset_index().set_index(['hisp_race', 'dateupdated'])
        aa_cases = race_df.loc[('NH Black',), 'case_tot'].dropna().astype(int)
        aa_deaths = race_df.loc[('NH Black',), 'deaths'].dropna().astype(int)
        unknown_cases = race_df.loc[('Unknown',),
                                    'case_tot'].dropna().astype(int)
        unknown_deaths = race_df.loc[('Unknown',),
                                     'deaths'].dropna().astype(int)

        # Compute denominators.
        #
        # Arithmetic on Series from DFs with different indices will
        # have the union of the indices, with the values for the
        # symmetric difference set to NaN.
        known_cases = total_cases - unknown_cases
        known_deaths = total_deaths - unknown_deaths

        # Compute the AA case/death percentages.
        pct_aa_cases = to_percentage(aa_cases, known_cases)
        pct_aa_deaths = to_percentage(aa_deaths, known_deaths)

        return self._make_dataframe(
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            status=SUCCESS
        )
