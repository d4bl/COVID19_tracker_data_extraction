import datetime
import logging
import re
from urllib.parse import urljoin

import pandas as pd
from tabula import read_pdf

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import find_all_links
from covid19_scrapers.utils.http import download_file
from covid19_scrapers.utils.misc import as_list, to_percentage


_logger = logging.getLogger(__name__)


class Mississippi(ScraperBase):
    """Mississippi uploads a PDF file with demographic breakdowns of
    COVID-19 cases and deaths daily. We scrape the reporting page for
    the latest PDF URL, and extract the tables from it.

    """
    REPORTING_URL = 'https://msdh.ms.gov/msdhsite/_static/14,0,420,884.html'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Find the PDF links
        title_dict = find_all_links(url=self.REPORTING_URL,
                                    search_string='pdf',
                                    links_and_text=True)

        # Dictionary from dates associated to their PDF links
        links_by_date = {}
        for link, title in title_dict.items():
            if 'Incidence and Cases' not in title:
                # Some entries omit the comma in the as-of date
                month, day, year = re.search(r'as of ([A-Z][a-z]+) (\d+),? (\d+)',
                                             title).groups()
                dt = datetime.datetime.strptime(f'{month} {day}, {year}',
                                                '%B %d, %Y').date()
                links_by_date[dt] = link

        # Find the most recent link
        date = max(links_by_date)
        pdf_link = links_by_date[date]

        # Extract the date
        _logger.info(f'Processing data for {date}')

        # The PDF URL is relative to the reporting URL
        case_and_death_url = urljoin(self.REPORTING_URL, pdf_link)

        _logger.debug('Cases/deaths url: {}'.format(case_and_death_url))

        # Download the files
        download_file(case_and_death_url, 'ms_cases_and_deaths.pdf')
        # download_file(deaths_url, 'ms_deaths.pdf')

        # Extract the tables
        cases = as_list(read_pdf('ms_cases_and_deaths.pdf', pages=[1, 2]))
        deaths = as_list(read_pdf('ms_cases_and_deaths.pdf', pages=[3, 4]))

        # Tables span across multiple pages, so concatenate them row-wise
        cases = pd.concat(cases)
        deaths = pd.concat(deaths)

        # Fail if the format changes, so we know to fix the scraper
        title = cases.columns[~cases.columns.str.startswith('Unnamed')][0]
        assert title == 'Cases by Race and Ethnicity'

        # Fix headers
        cases.columns = cases.iloc[1, :].str.replace(r'\r', ' ').str.strip()
        cases = cases[~cases['County'].isnull()
                      & (cases['County'] != 'County')]
        cases = cases.set_index('County')
        cases = cases.astype(int)

        # Fail if the format changes, so we know to fix the scraper
        title = deaths.columns[~deaths.columns.str.startswith('Unnamed')][0]
        assert title == 'Deaths by Race and Ethnicity'

        # Fix headers
        deaths.columns = deaths.iloc[1, :].str.replace(r'\r', ' ').str.strip()
        deaths = deaths[~deaths['County'].isnull()
                        & (deaths['County'] != 'County')]
        deaths = deaths.set_index('County')
        deaths = deaths.astype(int)

        # Aggregate over ethnicities
        cases_agg = (cases.iloc[:, 1:7]
                     + cases.iloc[:, 7:13]
                     + cases.iloc[:, 13:19])
        deaths_agg = (deaths.iloc[:, 1:7]
                      + deaths.iloc[:, 7:13]
                      + deaths.iloc[:, 13:19])

        # Copy over the totals
        cases_agg['Total'] = cases['Total Cases']
        deaths_agg['Total'] = deaths['Total Deaths']

        # Extract counts and compute percentages
        total_cases = cases_agg.loc['Total', 'Total']
        aa_cases = cases_agg.loc['Total', 'Black or African American']
        aa_cases_pct = to_percentage(aa_cases, total_cases)
        total_deaths = deaths_agg.loc['Total', 'Total']
        aa_deaths = deaths_agg.loc['Total', 'Black or African American']
        aa_deaths_pct = to_percentage(aa_deaths, total_deaths)

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
