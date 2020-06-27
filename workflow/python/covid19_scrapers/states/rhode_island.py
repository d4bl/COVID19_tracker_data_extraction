from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)


class RhodeIsland(ScraperBase):
    """Rhode Island publishes demographic breakdowns of COVID-19 case and
    death counts as a Google sheet.
    """

    DATA_URL = 'https://docs.google.com/spreadsheets/d/1n-zMS9Al94CPj_Tc3K7Adin-tN9x1RSjjx2UzJ4SV7Q/export?format=xlsx'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Rhode Island'

    def _scrape(self, **kwargs):
        data = pd.read_excel(self.DATA_URL, sheet_name='Demographics')

        # Get totals date
        month, day, year = map(int, re.search(
            r'(\d+)/(\d+)/(\d\d\d\d)',
            data.columns[0]
        ).groups())
        date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        # Get totals data
        total_cases = int(re.match(r'N=(\d+)',
                                   data.loc[0, 'Cases']).group(1))
        total_deaths = int(re.match(r'N=(\d+)',
                                    data.loc[0, 'Deaths']).group(1))

        data = data.set_index(data.columns[0])
        data = data.rename(columns={
            'Unnamed: 2': '% Cases',
            'Unnamed: 4': '% Hosp',
            'Unnamed: 6': '% Deaths',
        })
        for idx in data.index:
            str_idx = str(idx)
            if str_idx.startswith('Black'):
                aa_cases = int(data.loc[idx, 'Cases'])
                aa_deaths = int(data.loc[idx, 'Deaths'])
            elif str_idx.startswith('Unknown'):
                total_known_cases = total_cases - int(data.loc[idx, 'Cases'])
                total_known_deaths = total_deaths - int(data.loc[idx,
                                                                 'Deaths'])
        # Compute the percentages as the provided ones are excessively rounded.
        aa_cases_pct = round(100*aa_cases/total_known_cases, 2)
        aa_deaths_pct = round(100*aa_deaths/total_known_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
        )]
