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
    MS_REPORTING_URL = 'https://msdh.ms.gov/msdhsite/_static/14,0,420.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, validation):
        # Find the PDF links
        ms_soup = url_to_soup(self.MS_REPORTING_URL)
        race_table = ms_soup.find(id='raceTable').find_next_sibling('ul')
        ms_cases_url = urljoin(
            self.MS_REPORTING_URL,
            race_table.find(
                'a', text=re.compile('cases'))['href'])
        ms_deaths_url = urljoin(
            self.MS_REPORTING_URL,
            race_table.find(
                'a', text=re.compile('deaths'))['href'])

        # Download the files
        download_file(ms_cases_url, 'ms_cases.pdf')
        download_file(ms_deaths_url, 'ms_deaths.pdf')

        # Extract the date
        doc = fitz.Document(filename='ms_cases.pdf', filetype='pdf')
        for (
                x0, y0, x1, y1, word, block_no, line_no, word_no
        ) in doc[0].getText('words'):
            match = re.match(r'(\d+)/(\d+)/(\d+)', word)
            if match:
                month, day, year = map(int, match.groups())
                ms_date = datetime.date(year, month, day)
                break
        _logger.info(f'Report date is {ms_date}')

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
        ms_total_cases = cases_agg.loc['Total', 'Total']
        ms_aa_cases = cases_agg.loc['Total', 'Black or African American']
        ms_aa_cases_pct = round(100 * ms_aa_cases / ms_total_cases, 2)
        ms_total_deaths = deaths_agg.loc['Total', 'Total']
        ms_aa_deaths = deaths_agg.loc['Total', 'Black or African American']
        ms_aa_deaths_pct = round(100 * ms_aa_deaths / ms_total_deaths, 2)

        return [self._make_series(
            date=ms_date,
            cases=ms_total_cases,
            deaths=ms_total_deaths,
            aa_cases=ms_aa_cases,
            aa_deaths=ms_aa_deaths,
            pct_aa_cases=ms_aa_cases_pct,
            pct_aa_deaths=ms_aa_deaths_pct,
        )]
