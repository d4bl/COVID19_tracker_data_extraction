from covid19_scrapers.utils import get_esri_feature_data
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging


_logger = logging.getLogger(__name__)


class Missouri(ScraperBase):
    """Missouri has an ArcGIS dashboard that includes demographic
    breakdowns of confirmed cases and deaths.  We identified the
    underlying FeatureServer calls to populate this, and invoke those
    directly.

    The dashboard is at:
    http://mophep.maps.arcgis.com/apps/MapSeries/index.html?appid=8e01a5d8d8bd4b4f85add006f9e14a9d
    """

    TOTAL_CASE_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/lpha_boundry/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22Cases%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    TOTAL_DEATH_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/deaths_(1)/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Date%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true'
    RACE_CASE_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/COVID19_by_Race/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'
    RACE_DEATH_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/COVID19_Deaths_by_Race/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download and extract the case and death totals
        cases = get_esri_feature_data(self.TOTAL_CASE_URL)
        deaths = get_esri_feature_data(self.TOTAL_DEATH_URL)
        total_cases = cases.loc[0, 'value']
        # Deaths are sorted by Date ascending
        total_deaths = deaths.tail(1)['Cumulative_Case'].values[0]

        # Extract the date
        date = datetime.datetime.fromtimestamp(
            deaths.tail(1)['Date'] / 1000
        ).date()
        _logger.info(f'Processing data for {date}')

        # Extract by-race data
        cases_race = get_esri_feature_data(
            self.RACE_CASE_URL
        ).set_index('RACE')
        deaths_race = get_esri_feature_data(
            self.RACE_DEATH_URL
        ).set_index('RACE')
        aa_cases_cnt = cases_race.loc['BLACK', 'Frequency']
        aa_cases_pct = round(100 * cases_race.loc['BLACK', 'Percent'], 2)
        aa_deaths_cnt = deaths_race.loc['BLACK', 'Frequency']
        aa_deaths_pct = round(100 * deaths_race.loc['BLACK', 'Percent'],
                                 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
