import datetime
from io import BytesIO
import re

from github import Github
import logging
import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import to_percentage


_logger = logging.getLogger(__name__)


class NewYorkCity(ScraperBase):
    """
    """
    GITHUB_ORG = 'nychealth'
    GITHUB_REPO = 'coronavirus-data'

    def __init__(self, github_access_token=None, **kwargs):
        super().__init__(**kwargs)
        self.github_access_token = github_access_token

    def name(self):
        return 'New York -- New York'

    def _scrape(self, **kwargs):
        if self.github_access_token:
            _logger.debug('Using access token for Github API')
            github = Github(self.github_access_token)
        else:
            _logger.warn('Using unauthenticated for Github API: ' +
                         'be careful of hitting the rate limit')
            github = Github()
        org = github.get_organization(self.GITHUB_ORG)
        repo = org.get_repo(self.GITHUB_REPO)

        _logger.debug('Fetching latest summary data')
        latest_cases = pd.read_csv(
            BytesIO(repo.get_contents('summary.csv').decoded_content),
            header=None,
            names=['key', 'value'],
            index_col=0)

        # Get total cases.
        try:
            # Most recent row label
            total_cases = latest_cases.loc['NYC_CASE_COUNT', 'value']
        except KeyError:
            # Earlier row label
            total_cases = latest_cases.loc['Cases:', 'value']

        # Get total deaths.
        # TODO: should we include PROBABLE_DEATH_COUNT if available?
        try:
            # Most recent row label
            total_deaths = latest_cases.loc['NYC_CONFIRMED_DEATH_COUNT',
                                            'value']
        except KeyError:
            # Earlier row labels
            try:
                total_deaths = latest_cases.loc['Confirmed', 'value']
            except KeyError:
                total_deaths = latest_cases.loc['Deaths:', 'value']

        upload_re = re.compile(r'upload|update', re.I)
        date_re = re.compile(r'(\d+)[/.](\d+)')
        commits_by_date = {}
        for commit in repo.get_commits():
            if upload_re.search(commit.commit.message):
                match = date_re.search(commit.commit.message)
                if match:
                    year = datetime.date.today().year
                    month, day = map(int, match.groups())
                    date = datetime.date(year, month, day)
                    commits_by_date[date] = commit.commit.sha
                    if 'scrape_history' not in kwargs:
                        break
        max_date = max(commits_by_date)
        sha = commits_by_date[max_date]

        _logger.debug(f'Fetching demographic data for {max_date}')
        latest_demog = pd.read_csv(
            BytesIO(repo.get_contents('by-race.csv', ref=sha).decoded_content),
            index_col='RACE_GROUP'
        )
        aa_cases = latest_demog.loc['Black/African-American', 'CASE_COUNT']
        aa_deaths = latest_demog.loc['Black/African-American', 'DEATH_COUNT']

        known_df = latest_demog.sum(axis=0)
        known_cases = known_df['CASE_COUNT']
        known_deaths = known_df['DEATH_COUNT']

        aa_case_pct = to_percentage(aa_cases, known_cases)
        aa_death_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=max_date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_case_pct,
            pct_aa_deaths=aa_death_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
