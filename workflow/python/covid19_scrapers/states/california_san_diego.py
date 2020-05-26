from covid19_scrapers.utils import download_file, as_list
from covid19_scrapers.scraper import ScraperBase

from tabula import read_pdf

import datetime
import logging


_logger = logging.getLogger(__name__)


class CaliforniaSanDiego(ScraperBase):
    SD_CASES_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Race%20and%20Ethnicity%20Summary.pdf'
    SD_DEATHS_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Deaths%20by%20Demographics.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - San Diego'

    def _scrape(self, validation):
        # Download the files
        download_file(self.SD_CASES_URL, 'sd_cases.pdf')
        download_file(self.SD_DEATHS_URL, 'sd_deaths.pdf')

        # Load the cases
        sd_cases_raw = as_list(read_pdf('sd_cases.pdf'))[0]

        # Format the cases
        sd_cases = sd_cases_raw.drop(
            index=[0, 1]
        ).reset_index(
        ).drop(columns=['index'])
        sd_cases.columns = sd_cases.loc[0]
        sd_cases = sd_cases.drop(index=[0])
        sd_cases['Count'] = [int(x.replace(',', ''))
                             for x in sd_cases['Count']]
        sd_cases = sd_cases[['Race and Ethnicity', 'Count']]

        #
        sd_total_cases = sd_cases.Count.sum()
        _logger.debug(f'Total cases: {sd_total_cases}')
        sd_cases['Percent'] = round(100 * sd_cases['Count'] / sd_total_cases,
                                    2)

        #
        sd_cases = sd_cases.set_index('Race and Ethnicity')
        sd_aa_cases_cnt = sd_cases.loc['Black or African American', 'Count']
        sd_aa_cases_pct = sd_cases.loc['Black or African American', 'Percent']

        #
        sd_deaths_raw = as_list(read_pdf('sd_deaths.pdf'))[0]

        #
        sd_deaths = sd_deaths_raw.loc[19:, :].copy().reset_index().drop(
            columns=['index']
        ).dropna(how='all')

        def check_cvt(x):
            """Helper to log but skip conversion errors for counts.
            """
            try:
                return int(str(x).split()[0])
            except ValueError as e:
                _logger.warning(f'X is "{x}": {e}')
                return 0
        sd_deaths['Count'] = [check_cvt(x)
                              for x in sd_deaths['San Diego County Residents']
                              if x]
        del sd_deaths['San Diego County Residents']
        sd_deaths.columns = ['Race/Ethnicity', 'Count']

        #
        sd_total_deaths = sd_deaths.Count.sum()
        _logger.debug(sd_total_deaths)
        sd_deaths['Percent'] = round(
            100 * sd_deaths['Count'] / sd_total_deaths, 2
        )

        #
        sd_aa_deaths_cnt = sd_deaths.set_index(
            'Race/Ethnicity'
        ).loc['Black or African American', 'Count']
        sd_aa_deaths_pct = sd_deaths.set_index(
            'Race/Ethnicity'
        ).loc['Black or African American', 'Percent']
        sd_max_date = (
            datetime.datetime.now() - datetime.timedelta(days=1)
        )

        return [self._make_series(
            date=sd_max_date,
            cases=sd_total_cases,
            deaths=sd_total_deaths,
            aa_cases=sd_aa_cases_cnt,
            aa_deaths=sd_aa_deaths_cnt,
            pct_aa_cases=sd_aa_cases_pct,
            pct_aa_deaths=sd_aa_deaths_pct,
        )]
