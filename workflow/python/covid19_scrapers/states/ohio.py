from covid19_scrapers.utils import get_http_date, url_to_soup
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class Ohio(ScraperBase):
    """NOT DONE: Ohio changed from a custom website to a Tableau
    dashboard.  We need to finish this once we have a workflow for
    scraping from Tableau.
    """

    BETA_SCRAPER = True
    CASES_URL = 'https://public.tableau.com/profile/sohdoh#!/vizhome/KeyMetrics_15859581976410/DashboardKeyMetrics'
    DEATHS_URL = 'https://public.tableau.com/profile/sohdoh#!/vizhome/MortalityMetrics/DashboardMortalityMetrics'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        raise ValueError('Unable to scrape Ohio site')
        # Get date

        # Get totals data

        # Get AA case data

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
