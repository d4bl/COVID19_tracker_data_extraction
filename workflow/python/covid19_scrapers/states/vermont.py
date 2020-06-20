from covid19_scrapers.utils import (get_esri_feature_data,
                                    get_esri_metadata_date)
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Vermont(ScraperBase):
    METADATA_URL = 'https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/V_EPI_PositiveCases_PUBLIC/FeatureServer/0?f=json'
    TOTAL_URL = 'https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/V_EPI_DailyCount_PUBLIC/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20desc&resultOffset=0&resultRecordCount=1&resultType=standard&cacheHint=true'
    RACE_CASE_URL = 'https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/V_EPI_PositiveCases_PUBLIC/FeatureServer/0/query?f=json&where=Race%3C%3E%27%27%20AND%20Race%20NOT%20IN(%27Unknown%27)&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race&orderByFields=Race%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22OBJECTID_2%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    RACE_DEATH_URL = 'https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/V_EPI_PositiveCases_PUBLIC/FeatureServer/0/query?f=json&where=Death%3D%27Yes%27%20AND%20Race%20NOT%20IN(%27Unknown%27)&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22OBJECTID_2%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download the metadata
        date = get_esri_metadata_date(self.METADATA_URL)
        _logger.info(f'Processing data for {date}')

        # Download and extract total case and death data
        totals = get_esri_feature_data(self.TOTAL_URL)
        total_cases = totals.loc[0, 'cumulative_positives']
        total_deaths = totals.loc[0, 'total_deaths']

        # Download and extract AA case and death data
        cases = get_esri_feature_data(self.RACE_CASE_URL).set_index('Race')
        aa_cases_cnt = cases.loc['Black or African American', 'value']
        aa_cases_pct = round(100 * aa_cases_cnt / total_cases, 2)

        deaths = get_esri_feature_data(self.RACE_DEATH_URL).set_index('Race')
        try:
            aa_deaths_cnt = deaths.loc['Black or African American', 'value']
            aa_deaths_pct = round(100 * aa_deaths_cnt / total_deaths, 2)
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
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
