from datetime import datetime

import pydash

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_json
from covid19_scrapers.utils.misc import to_percentage


class Illinois(ScraperBase):
    """Illinois data could be obtained via the `DATA_URL` below.
    All the needed data is returned in json format.
    """
    DATA_URL = 'https://www.dph.illinois.gov/sitefiles/COVIDHistoricalTestResults.json?nocache=1'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        json = get_json(self.DATA_URL)

        state_info = pydash.get(json, 'state_testing_results.values.-1')
        demographics_data = pydash.get(json, 'demographics.race')
        aa_data = aa_data = pydash.find(
            demographics_data, lambda data: data['description'] == 'Black')

        date = datetime.strptime(state_info['testDate'], '%m/%d/%Y').date()
        cases = state_info.get('confirmed_cases')
        deaths = state_info.get('deaths')
        aa_cases = aa_data.get('count')
        aa_deaths = aa_data.get('deaths')

        assert cases, 'Could not find number of confirmed cases'
        assert deaths, 'Could not find number of deaths'
        assert aa_cases, 'Could not find number of AA cases'
        assert aa_deaths, 'Could not find number of AA deaths'

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
            pct_includes_hispanic_black=False
        )]
