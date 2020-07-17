from datetime import datetime
from functools import partial
import re

import pandas as pd
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, parse
from covid19_scrapers.utils.tableau import TableauParser
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Kansas(ScraperBase):
    """Kansas data is extracted from a Tableau dashboard

    The way this is extracted is by going to each of the Tableau tabs.
    When a request is made to the Tableau URL, several requests are made back and forth between the client and server.
    One of the requests made will contain a giant blob that contains 2 pieces of data in json

    The data can then be extracted through a custom parser. From there the needed data can be extracted.
    """
    SUMMARY_URL = 'https://public.tableau.com/views/COVID-19TableauVersion2/COVID-19Overview?:embed=y&:showVizHome=no'
    RACE_CASES_URL = 'https://public.tableau.com/views/COVID-19TableauVersion2/CaseCharacteristics?%3Aembed=y&%3AshowVizHome=no'
    RACE_DEATHS_URL = 'https://public.tableau.com/views/COVID-19TableauVersion2/DeathSummary?%3Aembed=y&%3AshowVizHome=no'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_date(self, soup):
        last_updated_text = soup.find(text='Last updated:')
        date_str = last_updated_text.find_next('span').text
        pattern = re.compile(r'(\d{2}\/\d{2}\/\d{4})')
        matches = pattern.search(date_str)
        assert matches, 'Date not found.'
        return datetime.strptime(matches.group(), '%m/%d/%Y').date()

    def to_df(self, json):
        df = pd.DataFrame.from_dict(json)
        df['Measure Values'] = df['Measure Values'].apply(parse.maybe_convert).replace('', 0)
        return df.astype({'Measure Values': float})

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()

        # Get date
        cases_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.SUMMARY_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(),'Last updated:')]"))
            .get_page_source())

        date = self.get_date(cases_results.page_source)

        # Cases for Race
        cases_by_race_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.RACE_CASES_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(),'Race Case Rates per 100,000')]"))
            .find_request('race_cases', find_by=lambda r: 'bootstrapSession' in r.path))

        assert cases_by_race_results.requests['race_cases'], 'No results for race_cases found'
        resp_body = cases_by_race_results.requests['race_cases'].response.body.decode('utf8')
        cases_for_race_json = TableauParser(resp_body).extract_data_from_key(key='Rates by Race for All Cases')
        df = self.to_df(cases_for_race_json)
        cases_df = df[(df['Measure Names'] == 'Number of Cases')]
        cases = int(cases_df['Measure Values'].sum())
        known_race_cases = int(cases_df[(cases_df['Race'] != 'Not Reported/Missing')]['Measure Values'].sum())
        aa_cases = int(cases_df[cases_df['Race'] == 'Black or African American']['Measure Values'].sum())

        # Deaths for Race
        deaths_by_race_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.RACE_DEATHS_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(),'Race Death Rates per 100,000')]"))
            .find_request('race_deaths', find_by=lambda r: 'bootstrapSession' in r.path))

        assert deaths_by_race_results.requests['race_deaths'], 'No results for race_deaths found'
        resp_body = deaths_by_race_results.requests['race_deaths'].response.body.decode('utf8')
        deaths_for_race_json = TableauParser(resp_body).extract_data_from_key(key='Mortality by Race')
        df = self.to_df(deaths_for_race_json)
        deaths_df = df[df['Measure Names'] == 'Number of Deaths']
        deaths = int(deaths_df['Measure Values'].sum())
        known_race_deaths = int(deaths_df[deaths_df['Race'] != 'Not Reported/Missing']['Measure Values'].sum())
        aa_deaths = int(deaths_df[deaths_df['Race'] == 'Black or African American']['Measure Values'].sum())

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
