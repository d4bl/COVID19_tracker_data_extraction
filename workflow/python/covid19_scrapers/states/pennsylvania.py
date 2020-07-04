import logging

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import (
    make_geoservice_stat, query_geoservice)
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Pennsylvania(ScraperBase):
    """Pennsylvania has an ArcGIS dashboard at
    https://experience.arcgis.com/experience/cfb3803eb93d42f7ab1c2cfccca78bf7

    We query the underlying FeatureServers to acquire the data.

    """
    CASES = dict(
        flc_url='https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Pennsylvania_Public_COVID19_Dashboard_Data/FeatureServer',
        layer_name='Public Health Dashboard',
        stats=[make_geoservice_stat('sum', 'Confirmed', 'value')],
    )

    DEATHS = dict(
        flc_url='https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Pennsylvania_Public_COVID19_Dashboard_Data/FeatureServer',
        layer_name='Public Health Dashboard',
        stats=[make_geoservice_stat('sum', 'Deaths', 'value')],
    )

    CASES_BY_RACE = dict(
        flc_url='https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Pennsylvania_Public_COVID19_Dashboard_Data/FeatureServer',
        layer_name='Race_Data',
        group_by='Race',
        stats=[make_geoservice_stat('sum', 'Positive_Cases', 'value')]
    )

    DEATHS_BY_RACE = dict(
        flc_url='https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/deathrace/FeatureServer',
        layer_name='deathrace',
        group_by='Race',
        stats=[make_geoservice_stat('sum', 'Deaths', 'value')],
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        date, cases = query_geoservice(**self.CASES)
        total_cases = cases.iloc[0, 0]

        _, cases_by_race = query_geoservice(**self.CASES_BY_RACE)
        cases_by_race = cases_by_race.set_index('Race')
        known_cases = total_cases - cases_by_race.loc['Not Reported',
                                                      'value']
        aa_cases = cases_by_race.loc['African American/Black', 'value']
        pct_aa_cases = to_percentage(aa_cases, known_cases)

        _, deaths = query_geoservice(**self.DEATHS)
        total_deaths = deaths.iloc[0, 0]

        _, deaths_by_race = query_geoservice(**self.DEATHS_BY_RACE)
        deaths_by_race = deaths_by_race.set_index('Race')
        known_deaths = deaths_by_race.drop('Not Reported',
                                           errors='ignore').sum()['value']
        aa_deaths = deaths_by_race.loc['African American', 'value']
        pct_aa_deaths = to_percentage(aa_deaths, known_deaths)

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
