from covid19_scrapers.utils import url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import re


_logger = logging.getLogger(__name__)


class NorthCarolina(ScraperBase):
    BETA_SCRAPER = True
    # TODO figure out how to download the crosstab from
    # https://covid19.ncdhhs.gov/dashboard/about-data
    REPORTING_URL = 'https://public.tableau.com/views/NCDHHS_COVID-19_DataDownload/Demographics?:embed=y&:toolbar=n&:embed_code_version=3&:loadOrderID=0&:display_count=n&publish=yes&:origin=viz_share_link&:showVizHome=no'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'North Carolina'

    def _scrape(self, **kwargs):
        raise ValueError('Unable to scrape North Carolina')
        # find date

        # find total number of cases and deaths

        # find number of Black/AA cases and deaths

        return [self._make_series(
            date=date_obj,
            cases=num_cases,
            deaths=num_deaths,
            aa_cases=cnt_aa_cases,
            aa_deaths=cnt_aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
