import logging

import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage

_logger = logging.getLogger(__name__)


class California(ScraperBase):
    """California provides demographic breakdowns of COVID cases and
    deaths on their open data portal. The by-race breakdown does not
    include rows for unknown/unreported race.

    The COVID-19 datasets are at
    https://data.ca.gov/dataset/covid-19-cases

    """
    COUNTY_URL = 'https://data.ca.gov/dataset/590188d5-8545-4c93-a9a0-e230f0db7290/resource/926fd08f-cc91-4828-af38-bd45de97f8c3/download/statewide_cases.csv'
    RACE_URL = 'https://data.ca.gov/dataset/590188d5-8545-4c93-a9a0-e230f0db7290/resource/7e477adb-d7ab-4d4b-a198-dc4c6dc634c9/download/case_demographics_ethnicity.csv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, start_date, end_date, **kwargs):
        # For statewide totals, sum the latest county figures.
        total_df = pd.read_csv(
            self.COUNTY_URL,
            parse_dates=['date']
        ).groupby('date').sum().sort_index(ascending=False)
        if start_date is not None:
            subindex = total_df.index[(total_df.index >= start_date)
                                      & (total_df.index <= end_date)]
        else:
            subindex = [total_df.index[total_df.index <= end_date].max()]
        total_cases = total_df.loc[subindex, 'totalcountconfirmed'].astype(int)
        total_deaths = total_df.loc[subindex, 'totalcountdeaths'].astype(int)

        # CA demographic breakdowns use combined race & ethnicity.
        race_df = pd.read_csv(
            self.RACE_URL,
            index_col=['date', 'race_ethnicity'],
            parse_dates=['date']
        ).sort_index(ascending=False)
        dates = race_df.index.levels[0]
        if start_date is not None:
            subindex = dates[(dates >= start_date) & (dates <= end_date)]
        else:
            subindex = [dates[dates <= end_date].max()]
        race_df = race_df.loc[subindex]
        race_df.loc[:, 'cases'] = race_df.loc[:, 'cases'].astype(int)
        race_df.loc[:, 'deaths'] = race_df.loc[:, 'deaths'].astype(int)
        aa_cases = race_df.loc[(slice(None), 'Black'), 'cases']
        aa_deaths = race_df.loc[(slice(None), 'Black'), 'deaths']

        # Since the above DF does not include unknown counts, we can
        # sum the rows to get known counts.
        known_df = race_df.reset_index().groupby('date').sum()
        known_cases = known_df.loc[:, 'cases'].astype(int)
        known_deaths = known_df.loc[:, 'deaths'].astype(int)

        dates = total_df.index.intersection(known_df.index)

        aa_cases_pct = to_percentage(aa_cases, known_cases)
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=total_cases[date],
            deaths=total_deaths[date],
            aa_cases=aa_cases[(date, 'Black')],
            aa_deaths=aa_deaths[(date, 'Black')],
            pct_aa_cases=aa_cases_pct[(date, 'Black')],
            pct_aa_deaths=aa_deaths_pct[(date, 'Black')],
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases[date],
            known_race_deaths=known_deaths[date],
        ) for date in dates]
