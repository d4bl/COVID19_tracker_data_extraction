from covid19_scrapers.utils import (
    make_geoservice_args, make_geoservice_stat, query_geoservice,
    to_percentage)
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Vermont(ScraperBase):
    """Vermont reports COVID-19 demographic breakdowns of cases and deaths
    on their ArcGIS dashboard. The dashboard is at
    https://experience.arcgis.com/experience/85f43bd849e743cb957993a545d17170
    """

    # Services are under https://services1.arcgis.com/BkFxaEFNwHqX3tAw
    TOTALS = make_geoservice_args(
        flc_id='94479a6d67fc406999c9b66dec7d4adb',
        layer_name='V_EPI_DailyCount_PUBLIC',
        out_fields=[
            'date',
            'cumulative_positives as Cases',
            'total_deaths as Deaths',
        ],
        limit=1,
    )

    RACE_CASE = make_geoservice_args(
        flc_id='0e6f8a6aeb084acaa5f7973e556cf708',
        layer_name='V_EPI_PositiveCases_PUBLIC',
        group_by='Race',
        stats=[
            make_geoservice_stat('count', 'OBJECTID_2', 'Cases'),
        ]
    )
    RACE_DEATH = make_geoservice_args(
        flc_id='0e6f8a6aeb084acaa5f7973e556cf708',
        layer_name='V_EPI_PositiveCases_PUBLIC',
        where="Death='Yes'",
        group_by='Race',
        stats=[
            make_geoservice_stat('count', 'OBJECTID_2', 'Deaths'),
        ]
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download the metadata
        date, totals = query_geoservice(**self.TOTALS)
        _logger.info(f'Processing data for {date}')

        # Download and extract total case and death data
        total_cases = totals.loc[0, 'Cases']
        total_deaths = totals.loc[0, 'Deaths']

        # Download and extract AA case and death data
        _, cases = query_geoservice(**self.RACE_CASE)
        cases = cases.set_index('Race')
        aa_cases_cnt = cases.loc['Black or African American', 'Cases']
        known_cases = cases.drop('Unknown').sum()['Cases']
        aa_cases_pct = to_percentage(aa_cases_cnt, known_cases)

        _, deaths = query_geoservice(**self.RACE_DEATH)
        deaths = deaths.set_index('Race')
        try:
            aa_deaths_cnt = deaths.loc['Black or African American', 'value']
            known_deaths = deaths.drop('Unknown').sum()['Deaths']
            aa_deaths_pct = to_percentage(aa_deaths_cnt, known_deaths)
        except KeyError:
            aa_deaths_cnt = 0
            aa_deaths_pct = 0

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
