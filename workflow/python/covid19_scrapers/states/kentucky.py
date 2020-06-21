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
        _logger.info(f'Report date is {date}')

        # Extract the tables
        table_list = read_pdf(
            'report.pdf',
            multiple_tables=True, pages=[1, 2])

        # Extract the data
        pct_re = re.compile(r'([0-9.]+)%?')
        seen = set()
        for table in table_list:
            cell_0_0 = table.iloc[0, :].astype(str)[0].replace('\r', ' ')
            if cell_0_0 in seen:
                continue
            seen.add(cell_0_0)

            if cell_0_0.startswith('Total Cases'):
                total_cases = int(table.iloc[0, 1].replace(',', ''))
                for row in range(1, table.shape[0]):
                    cell = table.iloc[row, 0]
                    if cell.startswith('Total Deaths'):
                        total_deaths = int(
                            table.iloc[row, 1].replace(',', ''))
                        break
            elif cell_0_0.startswith('Race of Cases'):
                for row in range(0, table.shape[0]):
                    cell = table.iloc[row, 0]
                    if not pd.isnull(cell):
                        if cell.find('Total Known') >= 0:
                            cases_known_pct = float(
                                pct_re.search(cell).group(1))
                        elif cell.startswith('Black'):
                            aa_cases_pct = float(
                                pct_re.search(cell).group(1))
                            break
            elif cell_0_0.startswith('Race of Deaths'):
                for row in range(0, table.shape[0]):
                    cell = table.iloc[row, 0]
                    if not pd.isnull(cell):
                        if cell.find('Total Known') >= 0:
                            deaths_known_pct = float(
                                pct_re.search(cell).group(1))
                        elif cell.startswith('Black'):
                            aa_deaths_pct = float(
                                pct_re.search(cell).group(1))
                            break

        # Compute the approximate counts
        aa_cases = round(total_cases *
                         aa_cases_pct / 100 *
                         cases_known_pct / 100)
        aa_deaths = round(total_deaths *
                          aa_deaths_pct / 100 *
                          deaths_known_pct / 100)

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
