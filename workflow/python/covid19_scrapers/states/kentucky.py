from covid19_scrapers.scraper import ScraperBase

import fitz
from tabula import read_pdf

import datetime
import logging
import re


_logger = logging.getLogger(__name__)


class Kentucky(ScraperBase):
    KY_REPORT_URL = 'https://chfs.ky.gov/agencies/dph/covid19/COVID19DailyReport.pdf'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Download the files
        download_file(self.KY_REPORT_URL, 'ky_report.pdf')

        # Extract the date
        doc = fitz.Document(filename='ky_report.pdf', filetype='pdf')
        for (
                x0, y0, x1, y1, word, block_no, line_no, word_no
        ) in doc[0].getText('words'):
            match = re.match(r'(\d+)/(\d+)/(\d+)', word)
            if match:
                month, day, year = map(int, match.groups())
                ky_date = datetime.date(year, month, day)
                break
        _logger.info(f'Report date is {ky_date}')

        # Extract the tables
        ky_table_list = read_pdf(
            'https://chfs.ky.gov/agencies/dph/covid19/COVID19DailyReport.pdf',
            multiple_tables=True, pages=[1, 2])

        # Extract the data
        pct_re = re.compile(r'([0-9.]+)%')
        for table in ky_table_list:
            cell_0_0 = table.iloc[0, 0].replace('\r', ' ')
            if cell_0_0.startswith('Total Cases'):
                ky_total_cases = int(table.iloc[0, 1].replace(',', ''))
                for row in range(1, table.shape[0]):
                    if table.iloc[row, 0].startswith('Total Deaths'):
                        ky_total_deaths = int(table.iloc[row, 1].replace(',',
                                                                         ''))
                        break
            elif cell_0_0.startswith('Race of Cases'):
                cases_known_pct = float(pct_re.search(cell_0_0).group(1))
                for row in range(1, table.shape[0]):
                    if table.iloc[row, 0].startswith('Black'):
                        ky_aa_cases_pct = float(table.iloc[row, 1][:-1])
                        break
            elif cell_0_0.startswith('Race of Deaths'):
                deaths_known_pct = float(pct_re.search(cell_0_0).group(1))
                for row in range(1, table.shape[0]):
                    if table.iloc[row, 0].startswith('Black'):
                        ky_aa_deaths_pct = float(table.iloc[row, 1][:-1])
                        break
        # Compute the approximate counts
        ky_aa_cases = round(ky_total_cases * ky_aa_cases_pct / 100)
        ky_aa_deaths = round(ky_total_deaths * ky_aa_deaths_pct / 100)

        return [self._make_series(
            date=ky_date,
            cases=ky_total_cases,
            deaths=ky_total_deaths,
            aa_cases=ky_aa_cases,
            aa_deaths=ky_aa_deaths,
            pct_aa_cases=ky_aa_cases_pct,
            pct_aa_deaths=ky_aa_deaths_pct,
        )]
