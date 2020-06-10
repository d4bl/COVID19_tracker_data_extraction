from covid19_scrapers.utils import get_esri_metadata_date, get_json
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd


_logger = logging.getLogger(__name__)


class Arkansas(ScraperBase):
    AR_DATA_URL = 'https://opendata.arcgis.com/datasets/ebf62bbdba59497a9dba00aed0c17078_0.geojson'
    AR_METADATA_URL = 'https://services1.arcgis.com/WzFsmainVTuD5KML/arcgis/rest/services/Demographic_Distribution_of_Confirmed_Cases/FeatureServer/0?f=json'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Arkansas'

    def _scrape(self, validation):
        # Download the metadata
        ar_date = get_esri_metadata_date(self.AR_METADATA_URL)
        
        # Download the case data
        ar_data = get_json(self.AR_DATA_URL)
        # Populate a DataFrame
        ar_cases = pd.DataFrame(
            [feature['properties'] for feature in ar_data['features']]
        ).set_index('Demographic')

        # Extract cells
        ar_total_cases = ar_cases.loc['Grand Total', 'All_Cases']
        ar_aa_cases_cnt = ar_cases.loc['Black', 'All_Cases']
        ar_aa_cases_pct = ar_cases.loc['Black', 'All_Cases_Percentage'][:-1]
        ar_total_deaths = ar_cases.loc['Grand Total', 'Deaths']
        ar_aa_deaths_cnt = ar_cases.loc['Black', 'Deaths']                
        ar_aa_deaths_pct = ar_cases.loc['Black', 'Deaths_Percentage'][:-1]

        return [self._make_series(
            date=ar_date,
            cases=ar_total_cases,
            deaths=ar_total_deaths,
            aa_cases=ar_aa_cases_cnt,
            aa_deaths=ar_aa_deaths_cnt,
            pct_aa_cases=ar_aa_cases_pct,
            pct_aa_deaths=ar_aa_deaths_pct,
        )]
