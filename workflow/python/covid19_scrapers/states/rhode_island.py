from covid19_scrapers.utils import (get_content_as_file,
                                    get_esri_feature_data,
                                    get_esri_metadata_date)
from covid19_scrapers.scraper import ScraperBase

from io import BytesIO
import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class RhodeIsland(ScraperBase):
    RACE_PCT_URL = 'https://static.dwcdn.net/data/R1HaD.csv'
    TOTAL_DEATHS_URL = 'https://services1.arcgis.com/dkWT1XL4nglP5MLP/arcgis/rest/services/COVID_Public_Map_TEST/FeatureServer/2/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22Covid_Deaths%22%2C%22outStatisticFieldName%22%3A%22Covid_Deaths_min%22%2C%22statisticType%22%3A%22min%22%7D%5D'
    TOTAL_CASES_URL = 'https://services1.arcgis.com/dkWT1XL4nglP5MLP/arcgis/rest/services/COVID_Public_Map_TEST/FeatureServer/2/query?f=json&where=1%3D1&outFields=*&returnGeometry=false&outStatistics=%5B%7B%22onStatisticField%22%3A%22Covid_case%22%2C%22outStatisticFieldName%22%3A%22Covid_case_min%22%2C%22statisticType%22%3A%22min%22%7D%5D'
    TOTAL_CASES_METADATA_URL = 'https://services1.arcgis.com/dkWT1XL4nglP5MLP/arcgis/rest/services/COVID_Public_Map_TEST/FeatureServer/2?f=json'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Rhode Island'

    def _scrape(self, validation):
        # Get totals date
        date = get_esri_metadata_date(self.TOTAL_CASES_METADATA_URL)
        _logger.info(f'Processing data for {date}')

        # Get totals data
        total_cases = get_esri_feature_data(
            self.TOTAL_CASES_URL
        ).iloc[0, 0]
        total_deaths = get_esri_feature_data(
            self.TOTAL_DEATHS_URL
        ).iloc[0, 0]

        # Download the by-race percentage data
        race_pct = pd.read_csv(
            get_content_as_file(self.RACE_PCT_URL)
        ).set_index(
            'Race/ethnicity'
        )
        aa_cases_pct = float(
            race_pct.loc['Non-Hispanic black/African American',
                         'Cases'][:-1])
        aa_deaths_pct = float(
            race_pct.loc['Non-Hispanic black/African American',
                         'Fatalities'][:-1])
        aa_cases = int(total_cases * aa_cases_pct / 100)
        aa_deaths = int(total_deaths * aa_deaths_pct / 100)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
        )]
