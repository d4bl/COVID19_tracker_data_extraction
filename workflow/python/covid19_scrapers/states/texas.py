from covid19_scrapers.utils import get_content
from covid19_scrapers.scraper import ScraperBase

import datetime
from io import BytesIO
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class Texas(ScraperBase):
    """Texas provides demographic breakdowns of COVID-19 case and death
    counts as an Excel spreadsheet, updated daily.
    """

    DATA_URL = 'https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        data = get_content(self.DATA_URL)
        cases_df = pd.read_excel(BytesIO(data),
                                 sheet_name='Cases by RaceEthnicity',
                                 header=None)
        deaths_df = pd.read_excel(BytesIO(data),
                                  sheet_name='Fatalities by Race-Ethnicity',
                                  header=None)

        # Extract publication date
        month, day = map(int, re.search(r'\b(\d+)/(\d+)\b',
                                        cases_df.iloc[0, 0]).groups())
        date = datetime.date(2020, month, day)
        _logger.info(f'Processing data for {date}')

        # Clean up dataframes and extract data
        cases_columns = cases_df.iloc[1, :]
        cases_df = cases_df.iloc[2:, :]
        cases_df.columns = cases_columns
        cases_df = cases_df.set_index('Race/Ethnicity')

        cnt_cases = cases_df.loc['Total', 'Number']
        cnt_cases_aa = cases_df.loc['Black', 'Number']
        pct_cases_aa = round(100 * cases_df.loc['Black', '%'], 2)

        deaths_columns = deaths_df.iloc[1, :]
        deaths_df = deaths_df.iloc[2:, :]
        deaths_df.columns = deaths_columns
        deaths_df = deaths_df.set_index('Race/Ethnicity')

        cnt_deaths = deaths_df.loc['Total', 'Number']
        cnt_deaths_aa = deaths_df.loc['Black', 'Number']
        pct_deaths_aa = round(100 * deaths_df.loc['Black', '%'], 2)

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
