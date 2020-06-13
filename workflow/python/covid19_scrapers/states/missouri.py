from covid19_scrapers.utils import get_esri_feature_data
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging


_logger = logging.getLogger(__name__)


class Missouri(ScraperBase):
    MO_TOTAL_CASE_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/lpha_boundry/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22Cases%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    MO_TOTAL_DEATH_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/deaths_(1)/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Date%20asc&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true'
    MO_RACE_CASE_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/COVID19_by_Race/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'
    MO_RACE_DEATH_URL = 'https://services6.arcgis.com/Bd4MACzvEukoZ9mR/arcgis/rest/services/COVID19_Deaths_by_Race/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download and extract the case and death totals
        mo_cases = get_esri_feature_data(self.MO_TOTAL_CASE_URL)
        mo_deaths = get_esri_feature_data(self.MO_TOTAL_DEATH_URL)
        mo_total_cases = mo_cases.loc[0, 'value']
        # Deaths are sorted by Date ascending
        mo_total_deaths = mo_deaths.tail(1)['Cumulative_Case'].values[0]

        # Extract the date
        mo_date = datetime.datetime.fromtimestamp(
            mo_deaths.tail(1)['Date'] / 1000
        ).date()
        _logger.info(f'Processing data for {mo_date}')

        # Extract by-race data
        mo_cases_race = get_esri_feature_data(
            self.MO_RACE_CASE_URL
        ).set_index('RACE')
        mo_deaths_race = get_esri_feature_data(
            self.MO_RACE_DEATH_URL
        ).set_index('RACE')
        mo_aa_cases_cnt = mo_cases_race.loc['BLACK', 'Frequency']
        mo_aa_cases_pct = round(100 * mo_cases_race.loc['BLACK', 'Percent'], 2)
        mo_aa_deaths_cnt = mo_deaths_race.loc['BLACK', 'Frequency']
        mo_aa_deaths_pct = round(100 * mo_deaths_race.loc['BLACK', 'Percent'],
                                 2)

        return [self._make_series(
            date=mo_date,
            cases=mo_total_cases,
            deaths=mo_total_deaths,
            aa_cases=mo_aa_cases_cnt,
            aa_deaths=mo_aa_deaths_cnt,
            pct_aa_cases=mo_aa_cases_pct,
            pct_aa_deaths=mo_aa_deaths_pct,
        )]
