from datetime import datetime
import re

import pandas as pd
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, powerbi
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class CaliforniaSanFrancisco(ScraperBase):
    """The San Francisco data comes from a PowerBI dashboard

    Request/Responses from PowerBI can be searched for and found such that
    the needed data can be extracted. This is done so with the `PowerBIParser` util
    """
    URL = 'https://app.powerbigov.us/view?r=eyJrIjoiMWEyYTRhYTAtMTdjYi00YTE1LWJiMTQtYTY3NmJmMjJhOThkIiwidCI6IjIyZDVjMmNmLWNlM2UtNDQzZC05YTdmLWRmY2MwMjMxZjczZiJ9&navContentPaneEnabled=false&filterPaneEnabled=false'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - San Francisco'

    def parse_date(self, date_string):
        pattern = re.compile(r'\d+\/\d+\/\d+')
        results = pattern.search(str(date_string))
        assert results, 'No date found'
        parsed_date = results.group()
        return datetime.strptime(parsed_date, '%m/%d/%Y').date()

    def parse_race_data(self, unparsed):
        return [up['C'] for up in unparsed if 'C' in up and len(up['C']) > 1]

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_presence_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='g']/*[name()='g']/*[name()='text']/*[name()='title' and contains(text(), 'Black')]"))
            .wait_for_visibility_of_elements((By.XPATH, "//button[span[contains(text(), 'Deaths')]]"))
            .find_request('date', find_by=powerbi.find_request_query(entity='Date_Uploaded'))
            .find_request('cases_by_race', find_by=powerbi.find_request_query(entity='Cases_Ethnicity'))
            .find_element_by_xpath("//button[span[contains(text(), 'Deaths')]]")
            .clear_request_history()
            .click_on_last_element_found()
            .wait_for_number_of_elements((By.XPATH, "//div[contains(@aria-label, 'Deaths -')]"), 6)
            .wait_for_number_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='rect']"), 22)
            .find_request('deaths_by_race', find_by=powerbi.find_request_query(entity='Deaths_Ethnicity')))

        assert 'date' in results.requests, '`date` request missing'
        assert 'cases_by_race' in results.requests, '`cases_by_race` request missing'
        assert 'deaths_by_race' in results.requests, '`deaths_by_race` request missing'

        # Date
        parser = powerbi.PowerBIParser(results.requests['date'])
        unparsed_date = parser.get_data_by_key(key='Date_Uploaded')[0]['M0']
        date = self.parse_date(unparsed_date)

        # Cases
        parser = powerbi.PowerBIParser(results.requests['cases_by_race'])
        unparsed_cases_by_race = parser.get_data_by_key(key='Cases_Ethnicity')
        cases_by_race = self.parse_race_data(unparsed_cases_by_race)
        cases_by_race_df = pd.DataFrame(cases_by_race, columns=['Race', 'Pct', 'Cases']).set_index('Race')
        cases = cases_by_race_df['Cases'].sum()
        aa_cases = cases_by_race_df.loc['Black or African American']['Cases']
        known_cases = cases - cases_by_race_df.loc['Unknown']['Cases']
        pct_aa_cases = misc.to_percentage(aa_cases, known_cases)

        # Deaths
        parser = powerbi.PowerBIParser(results.requests['deaths_by_race'])
        unparsed_deaths_by_race = parser.get_data_by_key(key='Deaths_Ethnicity')
        deaths_by_race = self.parse_race_data(unparsed_deaths_by_race)
        deaths_by_race_df = pd.DataFrame(deaths_by_race, columns=['Race', 'Deaths']).set_index('Race')
        deaths = deaths_by_race_df['Deaths'].sum()
        aa_deaths = deaths_by_race_df.loc['Black or African American']['Deaths']
        known_deaths = deaths - deaths_by_race_df.loc['Unknown']['Deaths']
        pct_aa_deaths = misc.to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
