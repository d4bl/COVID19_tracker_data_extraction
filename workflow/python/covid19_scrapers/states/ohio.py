from covid19_scrapers.scraper import ScraperBase

import logging


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
