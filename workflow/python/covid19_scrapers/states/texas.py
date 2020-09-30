import datetime
from io import BytesIO
import logging
import re

import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_content
from covid19_scrapers.utils.html import url_to_soup


_logger = logging.getLogger(__name__)


class Texas(ScraperBase):
    """Texas provides demographic breakdowns of COVID-19 case and death
    counts as an Excel spreadsheet, updated daily.
    """
    METADATA_URL = 'https://dshs.texas.gov/coronavirus/additionaldata/'
    DATA_URL = 'https://dshs.texas.gov/coronavirus/TexasCOVID19Demographics.xlsx.asp'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Extract publication date
        soup = url_to_soup(self.METADATA_URL)
        heading = soup.find('a', href='/coronavirus/TexasCOVID19Demographics.xlsx.asp').parent
        month, day, year = map(
            int, re.search(r'(\d\d?)/(\d\d?)/(\d\d\d\d)', heading.text).groups())
        date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        data = get_content(self.DATA_URL)
        cases_df = pd.read_excel(BytesIO(data),
                                 sheet_name='Cases by RaceEthnicity',
                                 header=0, index_col=0)

        cnt_cases = cases_df.loc['Total', 'Number']
        cnt_cases_aa = cases_df.loc['Black', 'Number']
        pct_cases_aa = round(cases_df.loc['Black', '%'], 2)

        deaths_df = pd.read_excel(BytesIO(data),
                                  sheet_name='Fatalities by Race-Ethnicity',
                                  header=0, index_col=0)
        deaths_df.index = deaths_df.index.str.strip()
        cnt_deaths = deaths_df.loc['Total', 'Number']
        cnt_deaths_aa = deaths_df.loc['Black', 'Number']
        pct_deaths_aa = round(deaths_df.loc['Black', '%'], 2)

        return [self._make_series(
            date=date,
            cases=cnt_cases,
            deaths=cnt_deaths,
            aa_cases=cnt_cases_aa,
            aa_deaths=cnt_deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
