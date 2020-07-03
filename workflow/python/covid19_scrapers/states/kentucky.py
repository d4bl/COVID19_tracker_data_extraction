from covid19_scrapers.utils import download_file
from covid19_scrapers.scraper import ScraperBase

import fitz
from tabula import read_pdf

import datetime
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class Kentucky(ScraperBase):
    """Kentucky updates a PDF report daily containing total cases and
    deaths, percent of cases and deaths with race known, and percent
    of cases and deaths by race where race is known.

    We use these to compute approximate Black/AA case/death counts.
    """

    REPORT_URL = 'https://chfs.ky.gov/agencies/dph/covid19/COVID19DailyReport.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download the files
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
                break
        _logger.info(f'Processing data for {date}')

        # Extract multiple tables
        table_list = read_pdf(
            'report.pdf',
            multiple_tables=True, pages=[1, 2], pandas_options={'header': None})

        # Extract the data from each
        pct_re = re.compile(r'([0-9.]+)%?')
        seen = set()
        for table in table_list:
            # Identify the table by upper left cell, since we can see
            # duplicates in some cases.
            cell_0_0 = table.iloc[0, 0]
            if pd.isnull(cell_0_0):
                table = table.iloc[1:]
                cell_0_0 = table.iloc[0, 0]

            cell_0_0 = str(cell_0_0).replace('\r', ' ')
            if cell_0_0 in seen:
                continue
            seen.add(cell_0_0)

            if cell_0_0.startswith('Total Cases'):
                # Summary table has total cases in row 0, and total
                # deaths somewhere below.
                total_cases = int(table.iloc[0, 1].replace(',', ''))
                for idx in range(1, table.shape[0]):
                    row = table.iloc[idx].astype(str)
                    if row[0].startswith('Total Deaths'):
                        total_deaths = int(row[1].replace(',', ''))
                        break
            elif cell_0_0.startswith('Race of Cases'):
                for idx in range(0, table.shape[0]):
                    row = table.iloc[idx].astype(str)
                    if row[0].find('Total Known') >= 0:
                        # % cases with race known is in col 0.
                        # Sometimes it is in row 0, other times row 1,
                        # hence checking in the loop.
                        cases_known_pct = float(
                            pct_re.search(row[0]).group(1))
                    elif row[0].startswith('Black'):
                        # % AA cases is in col 1.
                        aa_cases_pct = float(
                            pct_re.search(row[1]).group(1))
                        break
            elif cell_0_0.startswith('Race of Deaths'):
                for idx in range(0, table.shape[0]):
                    row = table.iloc[idx].astype(str)
                    if row[0].find('Total Known') >= 0:
                        # % deaths with race known is in col 0.
                        # Sometimes it is in row 0, other times row 1,
                        # hence checking in the loop.
                        deaths_known_pct = float(
                            pct_re.search(row[0]).group(1))
                    elif row[0].startswith('Black'):
                        # % AA deaths is in col 1.
                        aa_deaths_pct = float(
                            pct_re.search(row[1]).group(1))
                        break

        # Compute the approximate counts:
        # Since the AA% values do NOT include unknown race counts, we
        # need to omit these when backing out AA case/death counts
        # from the total.
        aa_cases = round(total_cases
                         * aa_cases_pct / 100
                         * cases_known_pct / 100)
        aa_deaths = round(total_deaths
                          * aa_deaths_pct / 100
                          * deaths_known_pct / 100)

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
