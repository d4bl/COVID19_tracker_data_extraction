import logging

import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_content_as_file
from covid19_scrapers.utils.misc import to_percentage

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

    def _scrape(self, refresh=False, **kwargs):
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
        most_recent_totals = totals_df.iloc[0]
        report_date = most_recent_totals.name.date()
        _logger.info(f'Processing data for {report_date}')
        total_cases = most_recent_totals['confirmedcases']
        total_deaths = most_recent_totals['confirmeddeaths']

        _logger.debug('Get race data')
        race_df = pd.read_json(
            get_content_as_file(self.RACE_DATA_URL),
            convert_dates=['dateupdated'],
            orient='records',
            dtype={'case_tot': float, 'deaths': float}
        ).set_index(
            ['dateupdated', 'hisp_race']
        )
        most_recent_race = race_df.loc[race_df.index.levels[0].max(), :]
        aa_cases = most_recent_race.loc['NH Black', 'case_tot']
        aa_deaths = most_recent_race.loc['NH Black', 'deaths']
        unknown_cases = most_recent_race.loc['Unknown', 'case_tot']
        unknown_deaths = most_recent_race.loc['Unknown', 'deaths']

        # Compute denominators.
        known_cases = total_cases - unknown_cases
        known_deaths = total_deaths - unknown_deaths

        # Compute the AA case/death percentages.
        pct_aa_cases = to_percentage(aa_cases, known_cases)
        pct_aa_deaths = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=report_date,
            cases=int(total_cases),
            deaths=int(total_deaths),
            aa_cases=int(aa_cases),
            aa_deaths=int(aa_deaths),
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            known_race_cases=int(known_cases),
            known_race_deaths=int(known_deaths),
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False
        )]
