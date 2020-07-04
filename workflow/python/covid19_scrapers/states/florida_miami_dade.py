import logging

from numpy import nan

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import query_geoservice
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class FloridaMiamiDade(ScraperBase):
    """Florida has an ArcGIS dashboard at
    https://experience.arcgis.com/experience/c2ef4a4fcbe5458fbf2e48a21e4fece9
    which includes county-level data, though with no demographic
    breakdown for deaths.

    We call the underlying FeatureServer to populate our data.

    """
    DEMOG = dict(
        flc_url='https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_COVID19_Cases/FeatureServer',
        layer_name='Florida_COVID_Cases',
        where="COUNTYNAME='DADE'",
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Florida -- Miami-Dade County'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'Florida',
                                county='Miami-Dade')

    def _scrape(self, **kwargs):
        date, data = query_geoservice(**self.DEMOG)
        _logger.info(f'Processing data for {date}')

        total_cases = data.loc[0, 'CasesAll']
        known_cases = total_cases - data.loc[0, 'C_RaceUnknown']
        aa_cases = data.loc[0, 'C_RaceBlack']
        pct_aa_cases = to_percentage(aa_cases, known_cases)

        total_deaths = data.loc[0, 'Deaths']
        # Does not include demographic breakdown of deaths
        known_deaths = nan
        aa_deaths = nan
        pct_aa_deaths = nan

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
