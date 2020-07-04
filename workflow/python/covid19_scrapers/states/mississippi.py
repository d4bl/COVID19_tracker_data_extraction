from covid19_scrapers.utils import (
    as_list, download_file, to_percentage, find_all_links, convert_date)
from covid19_scrapers.scraper import ScraperBase

# import fitz
from tabula import read_pdf

# import datetime
import logging
# import re
# from urllib.parse import urljoin
import pandas as pd


_logger = logging.getLogger(__name__)


class Mississippi(ScraperBase):
    """Mississippi updates PDF files with demographic breakdowns of
    COVID-19 cases and deaths daily. We scrape the reporting page for
    the latest URLs, and extract the tables from them.
    """

    REPORTING_URL = 'https://msdh.ms.gov/msdhsite/_static/14,0,420,884.html'
    BASE_URL = 'https://msdh.ms.gov/msdhsite/_static'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Find the PDF links
        # soup = url_to_soup(self.REPORTING_URL)
        title_dict = find_all_links(url=self.REPORTING_URL,
                                    search_string='pdf',
                                    links_and_text=True)

        # Dictionary of dates associated with the PDF links
        link_dates = {key: convert_date(val.replace('Mississippi COVID-19 Cases and Deaths as of ', ''))
                      for key, val in title_dict.items()}

        # Find the most recent link
        recent_link = {key: val for key, val in link_dates.items() if val == max(link_dates.values())}

        # Extract the date
        date = list(recent_link.values())[0]
        # _logger.info(f'Report date is {date}')

        # case_and_death_url = urljoin(self.BASE_URL, list(recent_link.keys())[0]) # didn't work for some reason
        case_and_death_url = '{}/{}'.format(self.BASE_URL, list(recent_link.keys())[0])

        print('Cases/deaths url: {}'.format(case_and_death_url))

        # Download the files
        download_file(case_and_death_url, 'ms_cases_and_deaths.pdf')
        # download_file(deaths_url, 'ms_deaths.pdf')

        # Extract the tables
        cases = as_list(read_pdf('ms_cases_and_deaths.pdf', pages=[1, 2]))
        deaths = as_list(read_pdf('ms_cases_and_deaths.pdf', pages=[3, 4]))

        # Tables span across multiple pages, so concatenate them row-wise
        cases = pd.concat(cases)
        deaths = pd.concat(deaths)

        # Fix headers
        cases.columns = cases.iloc[1, :].str.replace(r'\r', ' ').str.strip()
        cases = cases[~cases['County'].isnull()
                      & (cases['County'] != 'County')]
        cases = cases.set_index('County')
        cases = cases.astype(int)

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
