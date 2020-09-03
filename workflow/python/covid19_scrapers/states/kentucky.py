from collections import namedtuple
import datetime
import logging
import re

import fitz
from tabula import read_pdf

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import download_file
from covid19_scrapers.utils.misc import as_list
from covid19_scrapers.utils.parse import raw_string_to_int

_logger = logging.getLogger(__name__)
PCT_RE = re.compile(r'([0-9.]+)%?')
DemographicData = namedtuple(
    'DemographicData', ['known_pct', 'known_count', 'aa_pct', 'aa_count'])


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


def _extract_demographic_data(table, title, total_count, black_label):
    """Extract the relevant statistics from a demographic table."""
    _logger.debug('_extract_demographic_data args are '
                  f'{(table, title, total_count, black_label)}')
    match = PCT_RE.search(title)
    assert match is not None, f'Did not find percentage in "{table.index.name}"'
    known_pct = float(match.group(1))
    _logger.debug(f'known_pct is {known_pct}')
    known_count = int(total_count * known_pct / 100)
    _logger.debug(f'known_count is {known_count}')
    aa_pct = table.loc[black_label, 'value']
    _logger.debug(f'aa_pct is {aa_pct}')
    aa_count = int(aa_pct / 100
                   * known_pct / 100
                   * total_count)
    _logger.debug(f'aa_count is {aa_count}')
    return DemographicData(known_pct, known_count, aa_pct, aa_count)


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
        for (
                x0, y0, x1, y1, word, block_no, line_no, word_no
        ) in doc[0].getText('words'):
            match = re.match(r'(\d+)/(\d+)/(\d+)', word)
            if match:
                month, day, year = map(int, match.groups())
                date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        # Extract totals data
        totals_list = as_list(read_pdf(
            'report.pdf',
            multiple_tables=True, pages=1,
            lattice=True,
            pandas_options={'header': None}))

        _logger.debug(f'First table is {totals_list[0]}')

        totals = totals_list[0]
        totals[0] = (totals[0]
                     .str.replace('*', '', regex=False)
                     .str.replace('\r', ' ', regex=False))
        totals.set_index(0, inplace=True)
        total_cases = raw_string_to_int(totals.loc['Total Cases', 1])
        total_deaths = raw_string_to_int(totals.loc['Total Deaths', 1])

        # Clean demographic data tables and extract data
        raw_tables = as_list(read_pdf(
            'report.pdf',
            lattice=True,
            multiple_tables=True, pages=[2],
            pandas_options={'header': None}))
        seen = set()
        _logger.debug(f'got {len(raw_tables)} tables: ')
        for idx, table in enumerate(raw_tables):
            _logger.debug(f'table #{idx+1}: {table}')
            if len(table) == 0:
                continue
            table.iloc[:, 0] = (table.iloc[:, 0]
                                .str.replace('*', '', regex=False)
                                .str.replace('\r', ' ', regex=False))
            race_label = table.iloc[:, 0].str.contains(
                'Where Race Known').fillna(False)
            if race_label.any():
                splits = table[race_label].index.values.tolist() + [-1]
                for header, end in zip(splits[:-1], splits[1:]):
                    # Stash the table name
                    title = str(table.iloc[header, 0])
                    # Set up the table
                    tbl = table.iloc[header + 1:end].copy()
                    tbl.columns = ['race', 'value']
                    tbl.set_index('race', inplace=True)
                    tbl.loc[:, 'value'] = tbl.loc[:, 'value'].str.extract(
                        PCT_RE
                    ).astype(float)
                    # Find the Black/AA label (this has varied from
                    # report to report)
                    black_label = _get_black_label(tbl)
                    # Extract the data
                    if (title.find('Cases') >= 0 and 'cases' not in seen):
                        (known_case_pct, known_cases, aa_cases_pct,
                         aa_cases) = _extract_demographic_data(
                             tbl, title, total_cases, black_label)
                        seen.add('cases')
                    elif (title.find('Deaths') >= 0 and 'deaths' not in seen):
                        (known_death_pct, known_deaths, aa_deaths_pct,
                         aa_deaths) = _extract_demographic_data(
                             tbl, title, total_deaths, black_label)
                        seen.add('deaths')
        assert 'cases' in seen, 'Did not find Cases by Race table'
        assert 'deaths' in seen, 'Did not find Deaths by Race table'

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
