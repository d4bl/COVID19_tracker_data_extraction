import logging

import requests

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase, ERROR
from covid19_scrapers.utils.arcgis import (
    make_geoservice_stat, query_geoservice)
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class WisconsinMilwaukee(ScraperBase):
    """Milwaukee publishes COVID-19 demographic disaggregations on their
    ArcGIS dashboard at:
    https://www.arcgis.com/apps/opsdashboard/index.html#/018eedbe075046779b8062b5fe1055bf

    We retrieve the data from their FeatureServers.
    """

    # The services are at https://services5.arcgis.com/8Q02ELWlq5TYUASS
    CASES = dict(
        flc_id='73e2e7131f954bb6a1b0fbbd9dd53f5b',
        layer_name='Cases',
        group_by='Race_Eth',
        stats=[make_geoservice_stat('count', 'ObjectId', 'value')],
    )

    DEATHS = dict(
        flc_id='02f3b03e877e480ca5c2eb750dcbbc8c',
        layer_name='Deaths',
        group_by='Race_Eth',
        stats=[make_geoservice_stat('count', 'ObjectId', 'value')],
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Wisconsin -- Milwaukee'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'Wisconsin',
                                county='Milwaukee')

    def _scrape(self, **kwargs):
        # Get the timestamp
        date_published, cases_df = query_geoservice(**self.CASES)
        _logger.info(f'Processing data for {date_published}')
        cases_df = cases_df.set_index('Race_Eth')
        cases = cases_df['value'].sum()
        cases_unknown = cases_df.loc['Not Reported', 'value']
        known_cases = cases - cases_unknown

        _, deaths_df = query_geoservice(**self.DEATHS)
        deaths_df = deaths_df.set_index('Race_Eth')
        deaths = deaths_df['value'].sum()
        if 'Not Reported' in deaths_df.index:
            deaths_unknown = deaths_df.loc['Not Reported', 'value']
        else:
            deaths_unknown = 0
        known_deaths = deaths - deaths_unknown

        try:
            cases_aa = cases_df.loc['Black Alone', 'value']
            pct_cases_aa = to_percentage(cases_aa, known_cases)
        except IndexError:
            raise ValueError('Case counts for Black Alone not found')

        try:
            if 'Black Alone' in deaths_df.index:
                deaths_aa = deaths_df.loc['Black Alone', 'value']
            else:
                deaths_aa = 0
            pct_deaths_aa = to_percentage(deaths_aa, known_deaths)
        except IndexError:
            raise ValueError('Death counts for Black Alone not found')

        return [self._make_series(
            date=date_published,
            cases=cases,
            deaths=deaths,
            aa_cases=cases_aa,
            aa_deaths=deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]

    def _format_error(self, e):
        if isinstance(e, OverflowError):
            return f'{ERROR} ... processing last update timstamp: {repr(e)}'
        elif isinstance(e, requests.RequestException):
            return f'{ERROR} ... retrieving URL {e.request.url}: {repr(e)}'
        else:
            return super()._format_error(e)
