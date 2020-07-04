import logging

from numpy import nan

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import query_geoservice
from covid19_scrapers.utils.misc import to_percentage


def _make_florida_county_scraper(
        db_name,
        census_name,
        camel_case_name,
        snake_case_name):
    """Return a ScraperBase subclass that retrieves county level
    information for a Florida county.  This retrieves data from state
    dashboard, which does not include demographic breakdowns for
    deaths.

    It also creates a descriptive logger name.

    Arguments:

      db_name: the county name to use in the `where` clause of the
        ArcGIS query.  E.g., 'DADE' for Miami-Dade county.

      census_name: the county name to use in the scraper output in
        "Florida -- {} County". This MUST match the Census name for
        the county without the "County" suffix.  You can check these
        names at
        https://api.census.gov/data/2018/acs/acs5?get=NAME&for=county:*&in=state:12
        E.g., 'Miami-Dade' for Miami-Dade county.

      camel_case_name: the camel-case suffix to use in the class name,
        "Florida{camel_case_name}".  E.g., 'MiamiDade' for Miami-Dade
        county.

      snake_case_name: the snake-case suffix to use in the logger
        name, "florida_{snake_case_name}".  E.g., 'miami_dade' for
        Miami-Dade county.

    """
    _logger = logging.getLogger(
        __name__.replace('_county', f'_{snake_case_name}'))

    class FloridaCounty(ScraperBase):
        """Florida has an ArcGIS dashboard at
        https://experience.arcgis.com/experience/c2ef4a4fcbe5458fbf2e48a21e4fece9
        which includes county-level data, though with no demographic
        breakdown for deaths.

        We call the underlying FeatureServer to populate our data.

        """
        DEMOG = dict(
            flc_url='https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_COVID19_Cases/FeatureServer',
            layer_name='Florida_COVID_Cases',
            where=f"COUNTYNAME='{db_name}'",
        )

        def __init__(self, *, home_dir, census_api, **kwargs):
            super().__init__(home_dir=home_dir, census_api=census_api,
                             **kwargs)

        def name(self):
            return f'Florida -- {census_name} County'

        def _get_aa_pop_stats(self):
            return get_aa_pop_stats(self.census_api, 'Florida',
                                    county=census_name)

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

    FloridaCounty.__name__ = f'Florida{camel_case_name}'
    return FloridaCounty


# Create classes for the desired counties
FloridaMiamiDade = _make_florida_county_scraper('DADE', 'Miami-Dade',
                                                'MiamiDade', 'miami_dade')

FloridaOrange = _make_florida_county_scraper('ORANGE', 'Orange',
                                             'Orange', 'orange')
