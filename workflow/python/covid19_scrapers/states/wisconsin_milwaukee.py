from covid19_scrapers.utils import (
    make_geoservice_stat, query_geoservice, to_percentage)
from covid19_scrapers.scraper import ScraperBase, ERROR

import logging
import requests


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

    def _scrape(self, **kwargs):
        # Get the timestamp
        date_published, cases = query_geoservice(**self.CASES)
        _logger.info(f'Processing data for {date_published}')
        cases = cases.set_index('Race_Eth')
        cnt_cases = cases['value'].sum()
        cnt_cases_unknown = cases.loc['Not Reported', 'value']
        cnt_cases_known = cnt_cases - cnt_cases_unknown

        _, deaths = query_geoservice(**self.DEATHS)
        deaths = deaths.set_index('Race_Eth')
        cnt_deaths = deaths['value'].sum()
        if 'Not Reported' in deaths.index:
            cnt_deaths_unknown = deaths.loc['Not Reported', 'value']
        else:
            cnt_deaths_unknown = 0
        cnt_deaths_known = cnt_deaths - cnt_deaths_unknown

        try:
            cnt_cases_aa = cases.loc['Black Alone', 'value']
            pct_cases_aa = to_percentage(cnt_cases_aa, cnt_cases_known)
        except IndexError:
            raise ValueError('Case counts for Black Alone not found')

        try:
            if 'Black Alone' in deaths.index:
                cnt_deaths_aa = deaths.loc['Black Alone', 'value']
            else:
                cnt_deaths_aa = 0
            pct_deaths_aa = to_percentage(cnt_deaths_aa, cnt_deaths_known)
        except IndexError:
            raise ValueError('Death counts for Black Alone not found')

        return [self._make_series(
            date=date_published,
            cases=cnt_cases,
            deaths=cnt_deaths,
            aa_cases=cnt_cases_aa,
            aa_deaths=cnt_deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
        )]

    def _format_error(self, e):
        if isinstance(e, OverflowError):
            return f'{ERROR} ... processing last update timstamp: {repr(e)}'
        elif isinstance(e, requests.RequestException):
            return f'{ERROR} ... retrieving URL {e.request.url}: {repr(e)}'
        else:
            return super()._format_error(e)
