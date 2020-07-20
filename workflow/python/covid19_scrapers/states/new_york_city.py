import datetime
from io import BytesIO
import re

from github import Github, GithubException
import logging
import pandas as pd

from covid19_scrapers.census import get_aa_pop_stats
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class NewYorkCity(ScraperBase):
    """Retrieves NYC data from GitHub.

    Commits overwrite summary files, including demographic breakdowns,
    daily, so we use the GitHub API to identify report dates.

    """
    GITHUB_ORG = 'nychealth'
    GITHUB_REPO = 'coronavirus-data'

    def __init__(self, github_access_token=None, **kwargs):
        super().__init__(**kwargs)
        self.github_access_token = github_access_token

    def name(self):
        return 'New York -- New York'

    def _get_aa_pop_stats(self):
        return get_aa_pop_stats(self.census_api, 'New York',
                                city='New York')

    def _get_commits_in_date_range(self, repo, start_date, end_date):
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
                    if start_date is not None:
                        # Date range case.
                        if start_date <= date <= end_date:
                            # Prefer the "first" sha: commits are
                            # processed in reverse chronological order, so
                            # this will be the most recent.
                            if date not in commits_by_date:
                                commits_by_date[date] = commit.commit.sha
                        if date < start_date:
                            break
                    else:
                        # Single date case.
                        if date <= end_date:
                            # Prefer the "first" sha: commits are
                            # processed in reverse chronological order, so
                            # this will be the most recent.
                            if date not in commits_by_date:
                                commits_by_date[date] = commit.commit.sha
                                break
        return commits_by_date

    def _try_get_value(self, df, keys):
        for key in keys:
            try:
                return df.loc[key, 'value']
            except KeyError:
                pass
        raise KeyError(str(keys))

    def _get_totals(self, repo, sha):
        _logger.debug('Fetching summary data')
        summary = pd.read_csv(
            BytesIO(repo.get_contents('summary.csv', ref=sha).decoded_content),
            header=None,
            names=['key', 'value'],
            index_col=0)

        # Get total cases.
        total_cases = self._try_get_value(
            summary, keys=['NYC_CASE_COUNT', 'Cases:'])

        # Get total deaths.
        # TODO: should we include PROBABLE_DEATH_COUNT if available?
        total_deaths = self._try_get_value(
            summary, keys=['NYC_CONFIRMED_DEATH_COUNT', 'Confirmed', 'Deaths:',
                           'NYC confirmed deaths:'])
        # Return total counts
        return total_cases, total_deaths

    def _get_aa_covid_stats(self, repo, sha):
        _logger.debug('Fetching demographic data')
        demog = pd.read_csv(
            BytesIO(repo.get_contents('by-race.csv', ref=sha).decoded_content),
            index_col='RACE_GROUP'
        )
        aa_cases = demog.loc['Black/African-American', 'CASE_COUNT']
        aa_deaths = demog.loc['Black/African-American', 'DEATH_COUNT']

        known_df = demog.sum(axis=0)
        known_cases = known_df['CASE_COUNT']
        known_deaths = known_df['DEATH_COUNT']

        aa_case_pct = to_percentage(aa_cases, known_cases)
        aa_death_pct = to_percentage(aa_deaths, known_deaths)
        return (aa_cases, aa_deaths,
                aa_case_pct, aa_death_pct,
                known_cases, known_deaths)

    def _scrape_one_date(self, repo, date, sha):
        _logger.info(f'Processing data for {date}')
        total_cases, total_deaths = self._get_totals(repo, sha)
        try:
            (aa_cases, aa_deaths,
             aa_case_pct, aa_death_pct,
             known_cases, known_deaths) = self._get_aa_covid_stats(repo, sha)
        except GithubException as e:
            _logger.exception(e)
            (aa_cases, aa_deaths,
             aa_case_pct, aa_death_pct,
             known_cases, known_deaths) = [None] * 6

        return self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_case_pct,
            pct_aa_deaths=aa_death_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )

    def _scrape(self, start_date, end_date, **kwargs):
        if self.github_access_token:
            _logger.debug('Using access token for Github API')
            github = Github(self.github_access_token)
        else:
            _logger.warn('Using unauthenticated for Github API: '
                         'be careful of hitting the rate limit')
            github = Github()
        org = github.get_organization(self.GITHUB_ORG)
        repo = org.get_repo(self.GITHUB_REPO)

        commits_by_date = self._get_commits_in_date_range(
            repo,
            start_date,
            end_date)

        ret = []
        for date, sha in commits_by_date.items():
            try:
                ret.append(self._scrape_one_date(repo, date, sha))
            except Exception as e:
                ret.extend(self._handle_error(e, date=date))
        return ret
