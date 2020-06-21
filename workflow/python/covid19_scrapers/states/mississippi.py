from covid19_scrapers.utils import url_to_soup, download_file
from covid19_scrapers.scraper import ScraperBase

import fitz
from tabula import read_pdf

import datetime
import logging
import re
from urllib.parse import urljoin


_logger = logging.getLogger(__name__)


class Mississippi(ScraperBase):
    REPORTING_URL = 'https://msdh.ms.gov/msdhsite/_static/14,0,420.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Find the PDF links
        soup = url_to_soup(self.REPORTING_URL)
        race_table = soup.find(id='raceTable').find_next_sibling('ul')
        cases_url = urljoin(
            self.REPORTING_URL,
            race_table.find(
                'a', text=re.compile('cases'))['href'])
        deaths_url = urljoin(
            self.REPORTING_URL,
            race_table.find(
                'a', text=re.compile('deaths'))['href'])

        # Download the files
        download_file(cases_url, 'ms_cases.pdf')
        download_file(deaths_url, 'ms_deaths.pdf')

        # Extract the date
        doc = fitz.Document(filename='ms_cases.pdf', filetype='pdf')
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
        cases = read_pdf('ms_cases.pdf', pages=[1, 2])
        deaths = read_pdf('ms_deaths.pdf', pages=[1, 2])

        # Fix headers
        cases.columns = cases.iloc[1, :].str.replace(r'\r', ' ').str.strip()
        cases = cases[~cases['County'].isnull() &
                      (cases['County'] != 'County')]
        cases = cases.set_index('County')
        cases = cases.astype(int)

        deaths.columns = deaths.iloc[1, :].str.replace(r'\r', ' ').str.strip()
        deaths = deaths[~deaths['County'].isnull() &
                        (deaths['County'] != 'County')]
        deaths = deaths.set_index('County')
        deaths = deaths.astype(int)

        # Aggregate over ethnicities
        cases_agg = (cases.iloc[:, 1:7] +
                     cases.iloc[:, 7:13] +
                     cases.iloc[:, 13:19])
        deaths_agg = (deaths.iloc[:, 1:7] +
                      deaths.iloc[:, 7:13] +
                      deaths.iloc[:, 13:19])

        # Copy over the totals
        cases_agg['Total'] = cases['Total Cases']
        deaths_agg['Total'] = deaths['Total Deaths']

        # Extract counts and compute percentages
        total_cases = cases_agg.loc['Total', 'Total']
        aa_cases = cases_agg.loc['Total', 'Black or African American']
        aa_cases_pct = round(100 * aa_cases / total_cases, 2)
        total_deaths = deaths_agg.loc['Total', 'Total']
        aa_deaths = deaths_agg.loc['Total', 'Black or African American']
        aa_deaths_pct = round(100 * aa_deaths / total_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True,
        )]
