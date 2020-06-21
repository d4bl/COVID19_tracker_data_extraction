from covid19_scrapers.utils import get_esri_feature_data
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging


_logger = logging.getLogger(__name__)


class Wisconsin(ScraperBase):
    DATA_URL = 'https://services1.arcgis.com/ISZ89Z51ft1G16OK/ArcGIS/rest/services/COVID19_WI/FeatureServer/10/query?where=geo%3D%27STATE%27&objectIds=&time=&resultType=none&outFields=date%2C+positive%2C+deaths%2C+pos_blk%2C+dth_blk&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=date+desc&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=1&sqlFormat=none&f=json&token='

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        data = get_esri_feature_data(self.DATA_URL)

        date = datetime.datetime.fromtimestamp(data.loc[0, 'DATE']/1000).date()

        total_cases = data.loc[0, 'POSITIVE']
        total_deaths = data.loc[0, 'DEATHS']
        aa_cases = data.loc[0, 'POS_BLK']
        aa_deaths = data.loc[0, 'DTH_BLK']

        aa_cases_pct = round(100 * aa_cases / total_cases, 2)
        aa_deaths_pct = round(100 * aa_deaths / total_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
