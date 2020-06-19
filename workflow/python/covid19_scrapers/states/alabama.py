from covid19_scrapers.utils import (get_esri_feature_data,
                                    get_esri_metadata_date)
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Alabama(ScraperBase):
    METADATA_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/Statewide_COVID19_CONFIRMED_DEMOG_PUBLIC/FeatureServer/3?f=json'
    CASE_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/Statewide_COVID19_CONFIRMED_DEMOG_PUBLIC/FeatureServer/3/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Racecat&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22Race_Counts%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    DEATH_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/DIED_FROM_COVID19_STWD_DEMO_PUBLIC/FeatureServer/1/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Racecat&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22DiedFromCovid19%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download the metadata
        date = get_esri_metadata_date(self.METADATA_URL)

        # Download the case and death data as DataFrames
        cases = get_esri_feature_data(self.CASE_URL).set_index('Racecat')
        deaths = get_esri_feature_data(
            self.DEATH_URL
        ).set_index('Racecat')

        # Extract cells
        total_cases = cases.loc[:, 'value'].drop('Unknown').sum()
        aa_cases_cnt = cases.loc['Black', 'value']
        aa_cases_pct = round(100 * aa_cases_cnt / total_cases, 2)
        total_deaths = deaths.loc[:, 'value'].drop('Unknown').sum()
        aa_deaths_cnt = deaths.loc['Black', 'value']
        aa_deaths_pct = round(100 * aa_deaths_cnt / total_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
        )]
