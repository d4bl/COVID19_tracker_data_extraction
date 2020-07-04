import logging

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import (query_geoservice, to_percentage)


_logger = logging.getLogger(__name__)


class TexasBexar(ScraperBase):
    """Bexar County (San Antonio area) provides demographic breakdowns of
    COVID-19 cases and deaths on their ArcGIS dashboard. We call the
    corresponding FeatureServer APIs to retrieve the same data.  The
    dashboard is at
    https://cosagis.maps.arcgis.com/apps/opsdashboard/index.html#/d2c7584fe9fd4da1b30cb9d6cc311163
    """

    # Services are at https://services.arcgis.com/g1fRTDLeMgspWrYp
    TOTALS = dict(
        flc_id='94576453349c462598b2569e9d05d84c',
        layer_name='DateCOVID_Tracker',
        out_fields=['Date', 'ReportedCum as Cases', 'DeathsCum as Deaths'],
        order_by='Date desc',
        limit=1,
    )

    RACE = dict(
        flc_id='9ab036f1be9b401d88971e773e6d166f',
        layer_name='RaceEthnicity',
        out_fields=['RaceEthnicity', 'CasesConfirmed as Cases', 'Deaths'],
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Texas -- Bexar County'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'Texas',
                                county='Bexar')

    def _scrape(self, **kwargs):

        # Next get the cumulative case and death counts
        date_published, total = query_geoservice(**self.TOTALS)
        _logger.info(f'Processing data for {date_published}')

        try:
            cases = total.loc[0, 'Cases']
            deaths = total.loc[0, 'Deaths']
        except IndexError:
            raise ValueError('Total count data not found')

        # And finally the race/ethnicity breakdowns
        _, data = query_geoservice(**self.RACE)
        data = data.set_index('RaceEthnicity')

        try:
            known = data.sum()
            cases_aa = data.loc['Black', 'Cases']
            deaths_aa = data.loc['Black', 'Deaths']
            known_cases = known['Cases']
            known_deaths = known['Deaths']
            pct_cases_aa = to_percentage(cases_aa, known_cases)
            pct_deaths_aa = to_percentage(deaths_aa, known_deaths)
        except IndexError:
            raise ValueError('No data found for Black RaceEthnicity category')

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
