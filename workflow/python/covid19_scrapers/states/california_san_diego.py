import datetime
import logging
import re

import fitz
from tabula import read_pdf

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import download_file
from covid19_scrapers.utils.misc import (as_list, to_percentage)


_logger = logging.getLogger(__name__)


class CaliforniaSanDiego(ScraperBase):
    """San Diego updates two PDFs daily, with demographic summaries of
    COVID-19 cases and deaths. These include a disaggregation by
    combined race and ethnicity, with "Hispanic" as well as OMB race
    categories (which appear to only include non-Hispanic counts).
    """

    CASES_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Race%20and%20Ethnicity%20Summary.pdf'
    DEATHS_URL = 'https://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/Epidemiology/COVID-19%20Deaths%20by%20Demographics.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - San Diego'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'California',
                                county='San Diego')

    @staticmethod
    def check_cvt(x):
        """Helper to log but skip conversion errors for counts.
        """
        try:
            return int(str(x).split()[0])
        except ValueError as e:
            _logger.warning(f'X is "{x}": {e}')
            return 0

    def _scrape(self, **kwargs):
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

        _logger.debug('Loading cases')
        cases_raw = as_list(read_pdf('cases.pdf'))[0]

        # Scan the rows to find where the header ends.
        for idx in cases_raw.index:
            if cases_raw.iloc[idx, 0] == 'Race and Ethnicity':
                cases = cases_raw.iloc[idx + 1:].copy()
                cases.columns = cases_raw.iloc[idx]
                break

        # Format the cases and calculate/extract data.
        cases['Count'] = cases['Count'].str.replace(',', '').astype(int)
        cases = cases.set_index('Race and Ethnicity')

        total_cases = cases['Count'].sum()
        total_known_cases = cases['Count'].drop(
            'Race/Ethnicity Other/Unknown').sum()
        cases['Percent'] = to_percentage(cases['Count'], total_known_cases)

        aa_cases_cnt = cases.loc['Black or African American', 'Count']
        aa_cases_pct = cases.loc['Black or African American', 'Percent']

        _logger.debug(f'Total cases: {total_cases}')
        _logger.debug(f'Total cases with known race: {total_known_cases}')
        _logger.debug(f'Total AA cases: {aa_cases_cnt}')
        _logger.debug(f'Pct AA cases: {aa_cases_pct}')

        _logger.debug('Loading deaths')
        deaths_raw = as_list(read_pdf('deaths.pdf'))[0]

        # Scan the rows to find where the header ends.
        for idx in deaths_raw.index:
            if deaths_raw.iloc[idx, 0] == 'Total Deaths':
                # Pick out the total deaths en passant
                total_deaths = self.check_cvt(deaths_raw.iloc[idx, 1])
            elif deaths_raw.iloc[idx, 0] == 'Race/Ethnicity':
                deaths = deaths_raw.iloc[idx + 1:]
                # The table is read with two columns, and centering
                # makes some entries in the left column get included
                # in the right instead. dropna removes these.
                deaths = deaths.dropna().copy()
                deaths.columns = ['Race/Ethnicity', 'Count']
                break
        deaths = deaths.set_index('Race/Ethnicity')

        deaths['Count'] = deaths['Count'].apply(self.check_cvt)
        # Some reports have a discrepancy between sum of known
        # race/ethnicity counts, and total reported ex unknown
        # count. SD appears to use the latter, so we do the same.
        total_known_deaths = (
            total_deaths - deaths.loc['Race/Ethnicity Other/Unknown', 'Count'])

        deaths['Percent'] = to_percentage(deaths['Count'], total_known_deaths)

        aa_deaths_cnt = deaths.loc['Black or African American', 'Count']
        aa_deaths_pct = deaths.loc['Black or African American', 'Percent']

        _logger.debug(f'Total deaths: {total_deaths}')
        _logger.debug(f'Total deaths with known race: {total_known_deaths}')
        _logger.debug(f'Total AA deaths: {aa_deaths_cnt}')
        _logger.debug(f'Pct AA deaths: {aa_deaths_pct}')

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
            known_race_cases=total_known_cases,
            known_race_deaths=total_known_deaths,
        )]
