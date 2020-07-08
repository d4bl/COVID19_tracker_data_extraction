from datetime import datetime
import re

import pydash
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, parse, tableau
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Kansas(ScraperBase):
    CASES_AND_DEATHS_URL = 'https://public.tableau.com/views/COVID-19TableauVersion2/CaseSummary?%3Aembed=y&%3AshowVizHome=no'
    RACE_CASES_URL = 'https://public.tableau.com/views/COVID-19TableauVersion2/AgeandDemographics?%3Aembed=y&%3AshowVizHome=no'
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

    def create_lookup(self, json, indices, value_key):
        tupled_indices = zip(*[json[k] for k in indices])
        return {index: json[value_key][idx] for idx, index in enumerate(tupled_indices)}

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()

        # Total cases and total deaths
        cases_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_AND_DEATHS_URL)
            .wait_for_presence_of_elements([
                (By.XPATH, "//span[contains(text(),'Cases*')]"),
                (By.XPATH, "//span[contains(text(),'Statewide Deaths')]")
            ])
            .find_request('cases_and_deaths', find_by=lambda r: 'bootstrapSession' in r.path)
            .get_page_source())

        date = self.get_date(cases_results.page_source)

        assert cases_results.requests['cases_and_deaths'], 'No results for cases_and_deaths found'
        cases_json_list = tableau.extract_json_from_response(cases_results.requests['cases_and_deaths'].response)

        total_cases_json = tableau.extract_data_from_key(cases_json_list[1], key='Case Totals')
        assert 'CNT(Number of Records)' in total_cases_json, 'Missing total cases key.'
        cases = pydash.head(total_cases_json['CNT(Number of Records)'])

        total_deaths_json = tableau.extract_data_from_key(cases_json_list[1], key='Statewide Deaths')
        assert 'CNT(Number of Records)' in total_cases_json, 'Missing total deaths key.'
        deaths = pydash.head(total_deaths_json['CNT(Number of Records)'])

        # Cases for Race
        cases_by_race_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.RACE_CASES_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(),'Race Case Rates per 100,000')]"))
            .find_request('race_cases', find_by=lambda r: 'bootstrapSession' in r.path))

        assert cases_by_race_results.requests['race_cases'], 'No results for race_cases found'
        cases_for_race_data = tableau.extract_json_from_response(cases_by_race_results.requests['race_cases'].response)
        cases_for_race_json = tableau.extract_data_from_key(cases_for_race_data[1], key='Rates by Race for All Cases')
        lookup = self.create_lookup(cases_for_race_json, indices=['Race', 'Measure Names'], value_key='Measure Values')
        aa_cases = parse.raw_string_to_int(lookup[('Black or African American', 'Number of Cases')])

        # Deaths for Race
        deaths_by_race_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.RACE_DEATHS_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(),'Race Death Rates per 100,000')]"))
            .find_request('race_deaths', find_by=lambda r: 'bootstrapSession' in r.path))

        assert deaths_by_race_results.requests['race_deaths'], 'No results for race_deaths found'
        deaths_for_race_data = tableau.extract_json_from_response(deaths_by_race_results.requests['race_deaths'].response)
        deaths_for_race_json = tableau.extract_data_from_key(deaths_for_race_data[1], key='Mortality by Race')
        lookup = self.create_lookup(deaths_for_race_json, indices=['Race', 'Measure Names'], value_key='Measure Values')
        aa_deaths = parse.raw_string_to_int(lookup[('Black or African American', 'Number of Deaths')])

        pct_aa_cases = misc.to_percentage(aa_cases, cases)
        pct_aa_deaths = misc.to_percentage(aa_deaths, deaths)

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
        )]
