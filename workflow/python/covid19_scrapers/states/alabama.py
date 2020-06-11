from covid19_scrapers.utils import get_esri_feature_data, get_esri_metadata_date, get_json
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Alabama(ScraperBase):
    AL_METADATA_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/Statewide_COVID19_CONFIRMED_DEMOG_PUBLIC/FeatureServer/3?f=json'
    AL_CASE_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/Statewide_COVID19_CONFIRMED_DEMOG_PUBLIC/FeatureServer/3/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Racecat&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22Race_Counts%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'
    AL_DEATH_URL = 'https://services7.arcgis.com/4RQmZZ0yaZkGR1zy/arcgis/rest/services/DIED_FROM_COVID19_STWD_DEMO_PUBLIC/FeatureServer/1/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Racecat&outStatistics=%5B%7B%22statisticType%22%3A%22sum%22%2C%22onStatisticField%22%3A%22DiedFromCovid19%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download the metadata
        al_date = get_esri_metadata_date(self.AL_METADATA_URL)
        
        # Download the case and death data as DataFrames
        al_cases = get_esri_feature_data(self.AL_CASE_URL).set_index('Racecat')
        al_deaths = get_esri_feature_data(
            self.AL_DEATH_URL
        ).set_index('Racecat')

        # Extract cells
        al_total_cases = al_cases.loc[:, 'value'].sum()
        al_aa_cases_cnt = al_cases.loc['Black', 'value']
        al_aa_cases_pct = round(100 * al_aa_cases_cnt / al_total_cases, 2)
        al_total_deaths = al_deaths.loc[:, 'value'].sum()
        al_aa_deaths_cnt = al_deaths.loc['Black', 'value']         
        al_aa_deaths_pct = round(100 * al_aa_deaths_cnt / al_total_deaths, 2)

        return [self._make_series(
            date=al_date,
            cases=al_total_cases,
            deaths=al_total_deaths,
            aa_cases=al_aa_cases_cnt,
            aa_deaths=al_aa_deaths_cnt,
            pct_aa_cases=al_aa_cases_pct,
            pct_aa_deaths=al_aa_deaths_pct,
        )]
