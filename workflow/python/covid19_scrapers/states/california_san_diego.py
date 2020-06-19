from covid19_scrapers.utils import download_file, as_list
from covid19_scrapers.scraper import ScraperBase

import fitz
from tabula import read_pdf

import datetime
import logging
import re


_logger = logging.getLogger(__name__)


class CaliforniaSanDiego(ScraperBase):
    CASES_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Race%20and%20Ethnicity%20Summary.pdf'
    DEATHS_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Deaths%20by%20Demographics.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - San Diego'

    def _scrape(self, validation):
        # Download the files
        download_file(self.CASES_URL, 'cases.pdf')
        download_file(self.DEATHS_URL, 'deaths.pdf')

        # Extract the date
        pdf = fitz.Document(filename='cases.pdf', filetype='pdf')
        date = None
        for (
                x0, y0, x1, y1, block, block_type, block_no
        ) in pdf[0].getText('blocks'):
            match = re.search(r'updated +(\d\d?)/(\d\d?)/(\d{4})', block)
            if match:
                month, day, year = map(int, match.groups())
                date = datetime.date(year, month, day)
                break
        if not date:
            raise ValueError('Unable to find date in cases PDF')
        _logger.info(f'Processing data for {date}')

        # Load the cases
        cases_raw = as_list(read_pdf('cases.pdf'))[0]

        # Format the cases
        cases = cases_raw.drop(
            index=[0, 1]
        ).reset_index(
        ).drop(columns=['index'])
        cases.columns = cases.loc[0]
        cases = cases.drop(index=[0])
        cases['Count'] = [int(x.replace(',', ''))
                          for x in cases['Count']]
        cases = cases[['Race and Ethnicity', 'Count']]

        #
        total_cases = cases.Count.sum()
        _logger.debug(f'Total cases: {total_cases}')
        cases['Percent'] = round(100 * cases['Count'] / total_cases, 2)

        #
        cases = cases.set_index('Race and Ethnicity')
        aa_cases_cnt = cases.loc['Black or African American', 'Count']
        aa_cases_pct = cases.loc['Black or African American', 'Percent']

        #
        deaths_raw = as_list(read_pdf('deaths.pdf'))[0]

        #
        deaths = deaths_raw.loc[19:, :].copy().reset_index().drop(
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
        deaths['Count'] = [check_cvt(x)
                           for x in deaths['San Diego County Residents']
                           if x]
        del deaths['San Diego County Residents']
        deaths.columns = ['Race/Ethnicity', 'Count']

        #
        total_deaths = deaths.Count.sum()
        _logger.debug(total_deaths)
        deaths['Percent'] = round(
            100 * deaths['Count'] / total_deaths, 2
        )

        #
        aa_deaths_cnt = deaths.set_index(
            'Race/Ethnicity'
        ).loc['Black or African American', 'Count']
        aa_deaths_pct = deaths.set_index(
            'Race/Ethnicity'
        ).loc['Black or African American', 'Percent']

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
        )]
