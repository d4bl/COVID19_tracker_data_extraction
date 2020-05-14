from covid19_scrapers.utils import (get_esri_feature_data,
                                    get_esri_metadata_date)
from covid19_scrapers.scraper import ScraperBase, ERROR

import logging
import requests


_logger = logging.getLogger(__name__)


class WisconsinMilwaukee(ScraperBase):
    CASES_MD_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0?f=json'
    DEATHS_MD_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0?f=json'
    CASES_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    DEATHS_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    CASES_BY_RACE_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=Race_Eth%20NOT%20LIKE%20%27%25%23N%2FA%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    DEATHS_BY_RACE_URL = 'https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Wisconsin -- Milwaukee'

    def _scrape(self, validation):
        # Get the timestamp
        cases_date = get_esri_metadata_date(self.CASES_MD_URL)
        deaths_date = get_esri_metadata_date(self.DEATHS_MD_URL)
        if cases_date != deaths_date:
            _logger.debug(
                'Unexpected mismatch between cases and deaths metadata dates:', cases_date, '!=', deaths_date)
        date_published = cases_date

        cases_total = get_esri_feature_data(self.CASES_URL, ['value'])
        try:
            cnt_cases = cases_total.loc[0, 'value']
        except IndexError:
            raise ValueError('Total case count data not found')

        deaths_total = get_esri_feature_data(self.DEATHS_URL, ['value'])
        try:
            cnt_deaths = deaths_total.loc[0, 'value']
        except IndexError:
            raise ValueError('Total death count data not found')

        cases_by_race = get_esri_feature_data(self.CASES_BY_RACE_URL,
                                              ['Race_Eth', 'value'],
                                              ['Race_Eth'])
        try:
            cnt_cases_aa = cases_by_race.loc['Black Alone', 'value']
            pct_cases_aa = round(100 * cnt_cases_aa / cnt_cases, 2)
        except IndexError:
            raise ValueError('Case counts for Black Alone not found')

        deaths_by_race = get_esri_feature_data(self.DEATHS_BY_RACE_URL,
                                               ['Race_Eth', 'value'],
                                               ['Race_Eth'])
        try:
            cnt_deaths_aa = deaths_by_race.loc['Black Alone', 'value']
            pct_deaths_aa = round(100 * cnt_deaths_aa / cnt_cases, 2)
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
        )]

    def _format_error(self, e):
        if isinstance(e, OverflowError):
            return f'{ERROR} ... processing last update timstamp: {repr(e)}'
        elif isinstance(e, requests.RequestException):
            return f'{ERROR} ... retrieving URL {e.request.url}: {repr(e)}'
        else:
            return super()._format_error(e)
