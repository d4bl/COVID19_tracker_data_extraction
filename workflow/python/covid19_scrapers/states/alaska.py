from covid19_scrapers.utils import get_esri_metadata_date, get_json
from covid19_scrapers.scraper import ScraperBase

import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Arkansas(ScraperBase):
    DATA_URL = 'https://opendata.arcgis.com/datasets/ebf62bbdba59497a9dba00aed0c17078_0.geojson'
    METADATA_URL = 'https://services1.arcgis.com/WzFsmainVTuD5KML/arcgis/rest/services/Demographic_Distribution_of_Confirmed_Cases/FeatureServer/0?f=json'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download the metadata
        date = get_esri_metadata_date(self.METADATA_URL)

        # Download the case data
        data = get_json(self.DATA_URL)
        # Populate a DataFrame
        cases = pd.DataFrame(
            [feature['properties'] for feature in data['features']]
        ).set_index('Demographic')

        # Extract cells
        total_cases = cases.loc['Grand Total', 'All_Cases']
        aa_cases_cnt = cases.loc['Black', 'All_Cases']
        aa_cases_pct = cases.loc['Black', 'All_Cases_Percentage'][:-1]
        total_deaths = cases.loc['Grand Total', 'Deaths']
        aa_deaths_cnt = cases.loc['Black', 'Deaths']
        aa_deaths_pct = cases.loc['Black', 'Deaths_Percentage'][:-1]

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
