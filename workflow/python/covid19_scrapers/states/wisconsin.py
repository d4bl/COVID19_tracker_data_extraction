from covid19_scrapers.utils import query_geoservice
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging


_logger = logging.getLogger(__name__)


class Wisconsin(ScraperBase):
    """Wisconsin publishes COVID-19 demographic breakdowns of case and
    death counts on their ArcGIS dashboard at
    https://www.dhs.wisconsin.gov/covid-19/index.htm

    We retrieve the desired data with a custom call to their
    FeatureServer.
    """

    # Service is at https://services1.arcgis.com/ISZ89Z51ft1G16OK
    DATA = dict(
        flc_id='c38e9379b1c240bdaafa6195719c037d',
        layer_name='COVID19_WI_HIST',
        where="GEO='State'",
        out_fields=['DATE',
                    'POSITIVE', 'DEATHS',
                    'POS_BLK', 'POS_UNK',
                    'DTH_BLK', 'DTH_UNK'],
        order_by='DATE desc',
        limit=1,
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        date, data = query_geoservice(**self.DATA)

        total_cases = data.loc[0, 'POSITIVE']
        total_deaths = data.loc[0, 'DEATHS']
        unknown_cases = data.loc[0, 'POS_UNK']
        unknown_deaths = data.loc[0, 'DTH_UNK']
        known_cases = total_cases - unknown_cases
        known_deaths = total_deaths - unknown_deaths
        aa_cases = data.loc[0, 'POS_BLK']
        aa_deaths = data.loc[0, 'DTH_BLK']

        aa_cases_pct = round(100 * aa_cases / known_cases, 2)
        aa_deaths_pct = round(100 * aa_deaths / known_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
