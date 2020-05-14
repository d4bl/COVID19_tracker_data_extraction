from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Texas_Bexar(ScraperBase):
    MD_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0?f=json'
    TOTALS_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vDateCOVID19_Tracker_Public/FeatureServer/0/query?f=json&where=Date%20BETWEEN%20timestamp%20%272020-05-07%2005%3A00%3A00%27%20AND%20timestamp%20%272020-05-08%2004%3A59%3A59%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true'
    BY_RACE_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Texas -- Bexar County'
    
    def _scrape(self, validation):
        # Start by fetching the metadata to get the likey timestamp
        md_date = get_esri_metadata_date(self.MD_URL)
        date_published = str(md_date.strftime('%m/%d/%Y'))
        
        # Next get the cumulative case and death counts
        total = get_esri_feature_data(self.TOTALS_URL,
                                      ['ReportedCum', 'DeathsCum'])
        try:
            cnt_cases = total.loc[0, 'ReportedCum']
            cnt_deaths = total.loc[0, 'DeathsCum']
        except IndexError:
            raise ValueError('Total count data not found')
        
        # And finally the race/ethnicity breakdowns
        data = get_esri_feature_data(
            self.BY_RACE_URL,
            ['RaceEthnicity', 'CasesConfirmed', 'Deaths'],
            ['RaceEthnicity'])

        try:
            cnt_cases_aa = data.loc['Black', 'CasesConfirmed']
            cnt_deaths_aa = data.loc['Black', 'Deaths']
            pct_cases_aa = round(100 * cnt_cases_aa / cnt_cases, 2)
            pct_deaths_aa = round(100 * cnt_deaths_aa / cnt_deaths, 2)
        except IndexError:
            raise ValueError('No data found for Black RaceEthnicity category')

        return [self._make_series(
            date=date_published,
            cases=cnt_cases,
            deaths=cnt_deaths,
            aa_cases=cnt_cases_aa,
            aa_deaths=cnt_deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
        )]
