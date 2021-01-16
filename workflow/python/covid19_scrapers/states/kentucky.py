import datetime
import logging
import re

import fitz
import numpy as np
from tabula import read_pdf

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import download_file
from covid19_scrapers.utils.misc import as_list
from covid19_scrapers.utils.parse import raw_string_to_int

_logger = logging.getLogger(__name__)


def _get_black_label(tbl):
    """Find the first index entry containing the substring "Black".

    The KY report has used different labels for this data row, "Black"
    and "Black or African American", eg., so we must avoid hard-coding
    one.

    """
    for name in tbl.index:
        if str(name).find('Black') >= 0:
            return name
    raise RuntimeError('Unable to find Black/AA label in table '
                       f'{tbl.columns[0]}')


class Kentucky(ScraperBase):
    """Kentucky updates a PDF report daily containing total cases and
    deaths, percent of cases and deaths with race known, and percent
    of cases and deaths by race where race is known.

    We use these to compute approximate Black/AA case/death counts.
    """

    REPORT_URL = 'https://chfs.ky.gov/agencies/dph/covid19/COVID19DailyReport.pdf'
    REPORT_DATE_TEMPLATE = 'https://chfs.ky.gov/cvdaily/COVID19DailyReport{mm}{dd}.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download the report
        download_file(self.REPORT_URL, 'report.pdf')

        # Extract the date
        doc = fitz.Document(filename='report.pdf', filetype='pdf')
        date = None
        for (
                x0, y0, x1, y1, word, block_no, line_no, word_no
        ) in doc[0].getText('words'):
            if match := re.match(r'(\d{1,2}[A-Z]{3}\d{2})', word):
                date = datetime.datetime.strptime(match.group(0), '%d%b%y').date()
        if date is None:
            raise ValueError('Unable to find date in report')
        _logger.info(f'Processing data for {date}')

        for line in doc[0].getText().split('\n'):
            match = re.match(
                r'Race known for +([0-9.]+)% +of cases and +([0-9.]+)% +of deaths',
                line)
            if match:
                known_case_pct = float(match.group(1)) / 100
                known_death_pct = float(match.group(2)) / 100
                break
        else:
            raise ValueError('Report does not contain known-race percentages')

        # Extract totals data
        tables = as_list(read_pdf(
            'report.pdf',
            multiple_tables=True, pages=1,
            lattice=True,
            pandas_options={'header': None}))

        _logger.debug(f'First table is\n{tables[0]}')

        totals = tables[0].iloc[1:, :]
        totals.set_index(0, inplace=True)
        totals = totals.drop(columns=[1])
        totals.columns = totals.iloc[0, :]
        totals = totals.iloc[1:, :]
        totals = totals.applymap(raw_string_to_int)
        _logger.debug(f'Updated table is\n{totals}')

        total_cases = totals.loc['Cases', 'Total']
        total_deaths = totals.loc['Deaths', 'Total']

        known_cases = int(total_cases * known_case_pct)
        known_deaths = int(total_deaths * known_death_pct)

        # Clean demographic data tables and extract data
        _logger.debug(f'Got {len(tables)} tables')
        for idx, table in enumerate(tables[1:]):
            _logger.debug(f'Examining table #{idx+1}: {table}')
            race_idx = (table == 'Race')
            indices = np.argwhere(race_idx.values)
            if len(indices) == 0:
                _logger.debug(f'Table {idx+1} does not contain a "Race" cell')
                continue
            _logger.debug(f'"Race" is at cell(s): {indices}')
            race_x, race_y = indices[0]

            ethn_idx = (table == 'Ethnicity')
            indices = np.argwhere(ethn_idx.values)
            if len(indices) == 0:
                _logger.debug(f'Table {idx+1} does not contain a "Ethnicity" cell')
                continue
            _logger.debug(f'"Ethnicity" is at cell(s): {indices}')
            ethn_x, ethn_y = indices[0]

            subtable = table.iloc[race_x:ethn_x, race_y - 2:]
            subtable.columns = subtable.iloc[0, :]
            subtable.set_index('Race', inplace=True)
            # Find the Black/AA label (this has varied from
            # report to report)
            black_label = _get_black_label(subtable)
            # Extract the data
            aa_cases = subtable.loc[black_label, 'Cases']
            aa_deaths = subtable.loc[black_label, 'Deaths']
            # The state reports percentages as a fraction of
            # known-race cases and deaths.
            aa_cases_pct = float(subtable.loc[black_label, 'Case Percent'][:-1])
            aa_deaths_pct = float(subtable.loc[black_label, 'Death Percent'][:-1])
            break

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
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
