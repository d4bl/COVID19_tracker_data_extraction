import re
from datetime import datetime

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import tableau
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Oregon(ScraperBase):
    """Oregon data comes from a Tableau Dashboard

    We search requests for the response, then parse using the TableauParser, and extract the needed data.
    """
    URL = 'https://public.tableau.com/views/OregonCOVID-19CaseDemographicsandDiseaseSeverityStatewide-SummaryTable/DemographicDataSummaryTable?%3Aembed=y&%3AshowVizHome=no'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 12)
            .find_request('summary', find_by=tableau.find_tableau_request))

        parser = tableau.TableauParser(request=results.requests['summary'])
        date_str = parser.get_dataframe_from_key('Date Stamp').loc[0]['Date Stamp']
        match = re.search(r'\d{1,2}\/\d{1,2}\/\d{4}', date_str)
        date = datetime.strptime(match.group(), '%m/%d/%Y').date()

        cases_df = parser.get_dataframe_from_key('Demographic Data - Hospitalizaton Status')
        cases_df = cases_df[(cases_df['Status Value'] == '%all%') & (cases_df['Demographic'] == 'Race')].set_index('Categories')
        cases_df['SUM(Count)'] = cases_df['SUM(Count)'].apply(raw_string_to_int)
        cases = cases_df.loc['%all%', 'SUM(Count)']
        aa_cases = cases_df.loc['Black', 'SUM(Count)']
        known_race_cases = cases - cases_df.loc['Unknown', 'SUM(Count)'] - cases_df.loc['Refused', 'SUM(Count)']

        deaths_df = parser.get_dataframe_from_key('Demographic Data - Survival Outcomes')
        deaths_df = deaths_df[(deaths_df['Status Value'] == 'Died') & (deaths_df['Demographic'] == 'Race')].set_index('Categories')
        deaths_df['SUM(Count)'] = deaths_df['SUM(Count)'].apply(raw_string_to_int)
        deaths = deaths_df.loc['%all%', 'SUM(Count)']
        aa_deaths = deaths_df.loc['Black', 'SUM(Count)']
        known_race_deaths = deaths - deaths_df.loc['Unknown', 'SUM(Count)']

        pct_aa_cases = to_percentage(aa_cases, known_race_cases)
        pct_aa_deaths = to_percentage(aa_deaths, known_race_deaths)

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
