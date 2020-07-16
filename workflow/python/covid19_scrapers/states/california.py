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

    def _scrape(self, **kwargs):
        # For statewide totals, sum the latest county figures.
        total_df = pd.read_csv(
            self.COUNTY_URL,
            parse_dates=True
        ).groupby('date').sum().sort_index(ascending=False)
        total_cases = int(total_df.iloc[0]['totalcountconfirmed'])
        total_deaths = int(total_df.iloc[0]['totalcountdeaths'])

        # CA demographic breakdowns use combined race & ethnicity.
        race_df = pd.read_csv(
            self.RACE_URL,
            index_col=['date', 'race_ethnicity'],
            parse_dates=True
        ).sort_index(ascending=False)
        date = race_df.index.levels[0].max()
        race_df = race_df.loc[date]
        race_df['cases'] = race_df['cases'].astype(int)
        race_df['deaths'] = race_df['deaths'].astype(int)
        aa_cases = race_df.loc['Black', 'cases']
        aa_deaths = race_df.loc['Black', 'deaths']

        # Since the above DF does not include unknown counts, we can
        # sum the rows to get known counts.
        known_df = race_df.sum(axis=0)
        known_cases = int(known_df['cases'])
        known_deaths = int(known_df['deaths'])

        aa_cases_pct = to_percentage(aa_cases, known_cases)
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date.date(),
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
