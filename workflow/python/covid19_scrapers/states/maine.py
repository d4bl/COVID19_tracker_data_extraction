import logging
import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_content_as_file
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Maine(ScraperBase):
    """Maine publishes demographic breakdowns of COVID cases (not yet
    deaths) in CSV files. These files contain the up to date info only,
    no historical data is given.

    The homepage URL is:
    https://www.maine.gov/dhhs/mecdc/infectious-disease/epi/airborne/coronavirus/data.shtml
    """
    CASES_BY_COUNTY_URL = 'https://gateway.maine.gov/dhhs-apps/mecdc_covid/cases_by_county.csv'
    CASES_BY_RACE_URL = 'https://gateway.maine.gov/dhhs-apps/mecdc_covid/cases_by_race.csv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        counties = pd.read_csv(self.CASES_BY_COUNTY_URL)
        total_deaths = counties['DEATHS'].sum()

        table = pd.read_csv(
            get_content_as_file(self.CASES_BY_RACE_URL),
            index_col=0,
            parse_dates=['DATA_REFRESH_DT'])

        total_cases = table['CASES'].sum()
        known_cases = table['CASES'].drop('Not disclosed').sum()
        date = table['DATA_REFRESH_DT'][0].date()
        _logger.info(f'Processing data for {date}')
        aa_cases_cnt = table.loc['Black or African American', 'CASES']
        aa_cases_pct = to_percentage(aa_cases_cnt, known_cases)

        # No race breakdowns for deaths
        aa_deaths_cnt = float('nan')
        aa_deaths_pct = float('nan')
        known_deaths = float('nan')

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
