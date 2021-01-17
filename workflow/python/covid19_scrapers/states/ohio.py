from datetime import datetime

import pydash
import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps
from covid19_scrapers.utils import misc, tableau


class Ohio(ScraperBase):
    """COVID data for Ohio comes from 2 Tableau dashboards.

    The first dashboard contains Total Cases, Total Deaths and cases by racial breakdown
    The second dashboard contains deaths by racial breakdown
    """

    CASES_URL = 'https://public.tableau.com/views/KeyMetrics_15859581976410/DashboardKeyMetrics?%3Aembed=y&%3AshowVizHome=no'
    DEATHS_URL = 'https://public.tableau.com/views/MortalityMetrics/DashboardMortalityMetrics?%3Aembed=y&%3AshowVizHome=no'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_URL)
            .find_request('cases', find_by=tableau.find_tableau_request)
            .clear_request_history()
            .go_to_url(self.DEATHS_URL)
            .find_request('deaths', find_by=tableau.find_tableau_request))

        parser = tableau.TableauParser(request=results.requests['cases'])

        date_str = pydash.head(parser.extract_data_from_key('Footer')['AGG(Today)'])
        date = datetime.strptime(date_str, '%m-%d-%y').date()

        cases = pydash.head(parser.extract_data_from_key('Total Cases')['AGG(Total Cases)'])
        deaths = pydash.head(parser.extract_data_from_key('Total  Deaths')['SUM(Count Of Deaths)'])
        cases_pct_df = pd.DataFrame.from_dict(parser.extract_data_from_key('Race Breakdown ')).set_index('Race')
        cases_df = cases_pct_df.assign(Count=[round(v * cases) for v in cases_pct_df['CNTD(Caseid 1)'].values])
        aa_cases = cases_df.loc['Black']['Count']
        known_race_cases = cases - cases_df.loc['Unknown']['Count']

        parser = tableau.TableauParser(request=results.requests['deaths'])
        deaths_pct_df = pd.DataFrame.from_dict(parser.extract_data_from_key('Bar | Race')).set_index('Race')
        deaths_df = deaths_pct_df.assign(Count=[round(v * deaths) for v in deaths_pct_df['SUM(Death Count)'].values])
        aa_deaths = deaths_df.loc['Black']['Count']
        known_race_deaths = deaths - deaths_df.loc['Unknown']['Count']

        pct_aa_cases = misc.to_percentage(aa_cases, known_race_cases)
        pct_aa_deaths = misc.to_percentage(aa_deaths, known_race_deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_race_cases,
            known_race_deaths=known_race_deaths
        )]
