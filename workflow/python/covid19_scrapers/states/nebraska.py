import logging

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import (
    make_geoservice_stat, query_geoservice)
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)
_NE_SERVER_URL = 'https://gis.ne.gov/enterprise/rest/services/Covid19MapV5/MapServer'


class Nebraska(ScraperBase):
    """Nebraska publishes totals and by-race breakdowns on their ArcGIS
    dashboard at
    https://experience.arcgis.com/experience/ece0db09da4d4ca68252c3967aa1e9dd

    Nebraska uses ArcGIS Enterprise, and using their portal (search by
    ID) features requires authentication. Therefore we point directly
    to the MapServer providing data for the dashboard.

    """
    DATE = dict(
        flc_url=_NE_SERVER_URL,
        layer_name='covid19_hot_accumulative_lab_dt',
        out_fields=['LAB_REPORT_DATE'],
        order_by='LAB_REPORT_DATE desc',
        limit=1
    )

    TOTAL_CASES = dict(
        flc_url=_NE_SERVER_URL,
        layer_name='COVID19_COLD',
        where="lab_status='Positive' AND NE_JURIS='yes'",
        stats=[make_geoservice_stat('count', 'ID', 'value')]
    )

    TOTAL_DEATHS = dict(
        flc_url=_NE_SERVER_URL,
        layer_name='COVID19_CASE_COLD',
        where=' AND '.join(["case_status='Confirmed'",
                            "NE_JURIS='yes'",
                            "Did_Pat_Die_From_Illness='Y'"]),
        stats=[make_geoservice_stat('count', 'ID', 'value')]
    )

    DEMOG = dict(
        flc_url=_NE_SERVER_URL,
        layer_name='DHHS_GIS.DHHS.COVID19_RE_HORIZONTAL',
        order_by='Category desc'
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # NE does not version data, so there the update date is null.
        # We must query the date from one of the tables instead.
        _, date_df = query_geoservice(**self.DATE)
        date = date_df.loc[0, 'LAB_REPORT_DATE'].date()
        _logger.info(f'Processing data for {date}')

        _, total_cases_df = query_geoservice(**self.TOTAL_CASES)
        total_cases = total_cases_df.loc[0, 'value']

        _, total_deaths_df = query_geoservice(**self.TOTAL_DEATHS)
        total_deaths = total_deaths_df.loc[0, 'value']

        _, demog_df = query_geoservice(**self.DEMOG)
        demog_df = demog_df.set_index('Category')
        demog_df = demog_df[list(filter(
            lambda x: x.startswith('race_'),
            demog_df.columns))]

        known_df = demog_df.drop(columns=['race_Unknown']).sum(axis=1)

        aa_cases = demog_df.loc['PositiveCases', 'race_AfricanAmerican']
        aa_deaths = demog_df.loc['Deaths', 'race_AfricanAmerican']
        known_cases = known_df['PositiveCases']
        known_deaths = known_df['Deaths']
        aa_cases_pct = to_percentage(aa_cases, known_cases)
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=int(total_cases),
            deaths=int(total_deaths),
            aa_cases=int(aa_cases),
            aa_deaths=int(aa_deaths),
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
