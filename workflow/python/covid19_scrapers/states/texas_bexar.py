from covid19_scrapers.utils import (get_esri_feature_data,
                                    get_esri_metadata_date,
                                    to_percentage)
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class TexasBexar(ScraperBase):
    """Bexar County (San Antonio area) provides demographic breakdowns of
    COVID-19 cases and deaths on their ArcGIS dashboard. We call the
    corresponding FeatureServer APIs to retrieve the same data.  The
    dashboard is at
    https://cosagis.maps.arcgis.com/apps/opsdashboard/index.html#/d2c7584fe9fd4da1b30cb9d6cc311163
    """

    METADATA_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0?f=json'
    TOTALS_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vDateCOVID19_Tracker_Public/FeatureServer/0/query?f=json&where=Date%20BETWEEN%20timestamp%20%272020-05-07%2005%3A00%3A00%27%20AND%20timestamp%20%272020-05-08%2004%3A59%3A59%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=50&resultType=standard&cacheHint=true'
    BY_RACE_URL = 'https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/vRaceEthnicity/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&resultOffset=0&resultRecordCount=20&resultType=standard&cacheHint=true'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Texas -- Bexar County'

    def _scrape(self, **kwargs):
        # Start by fetching the metadata to get the likey timestamp
        date_published = get_esri_metadata_date(self.METADATA_URL)

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
            pct_cases_aa = to_percentage(cnt_cases_aa, cnt_cases)
            pct_deaths_aa = to_percentage(cnt_deaths_aa, cnt_deaths)
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
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
