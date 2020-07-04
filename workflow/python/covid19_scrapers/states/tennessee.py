import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_content_as_file
from covid19_scrapers.utils.misc import to_percentage


class Tennessee(ScraperBase):
    """The Tennessee can be downloaded via their dataset apis.

    Of the apis they have, the `daily cases` set can be used for
    obtaining the total cases/deaths while the `race, ethnic, sex` set
    can be used for obtaining aa cases/deaths info.

    The API returns the data back in xlsx format.

    """
    BASE_URL = 'https://www.tn.gov/content/dam/tn/health/documents/cedep/novel-coronavirus/datasets/{}'
    DEMOGRAPHIC_SUFFIX = 'Public-Dataset-RaceEthSex.XLSX'
    CASES_SUFFIX = 'Public-Dataset-Daily-Case-Info.XLSX'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        cases_df = pd.read_excel(get_content_as_file(
            self.BASE_URL.format(self.CASES_SUFFIX)))
        most_recent_cases = cases_df.iloc[-1]
        date = most_recent_cases['DATE'].to_pydatetime().date()
        cases = int(most_recent_cases['TOTAL_CASES'])
        deaths = int(most_recent_cases['TOTAL_DEATHS'])

        demographic_df = pd.read_excel(get_content_as_file(
            self.BASE_URL.format(self.DEMOGRAPHIC_SUFFIX)))
        most_recent_aa_cases = demographic_df[
            (demographic_df['Category'] == 'RACE')
            & (demographic_df['CAT_DETAIL'] == 'Black or African American')
            & (demographic_df['Date'] == str(date))
        ].iloc[0]
        aa_cases = int(most_recent_aa_cases['Cat_CaseCount'])
        aa_deaths = int(most_recent_aa_cases['CAT_DEATHCOUNT'])

        pct_aa_cases = to_percentage(aa_cases, cases)
        pct_aa_deaths = to_percentage(aa_deaths, deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=True
        )]
