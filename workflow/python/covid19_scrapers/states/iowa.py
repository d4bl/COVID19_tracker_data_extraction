from datetime import datetime
import json
import re

import pydash
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.parse import raw_string_to_int, maybe_convert
from covid19_scrapers.webdriver import WebdriverSteps, WebdriverRunner


class Iowa(ScraperBase):
    """Data for Iowa comes from Domo dashboards.

    The `CASES_DASHBOARD_URL` or `DEATHS_DASHBOARD_URL` brings the user to a page where dashboards are displayed
    The data in these dashboards are recieved in different urls as PUT requests.
    In each of those requests that returns dashboard data, there is a "card" number associated with each that does not change.
    By collecting the requests that are associated with the card number, the data from the requests can be extracted and parsed
    """
    CASES_DASHBOARD_URL = 'https://public.domo.com/embed/pages/aQVpq'
    CASES_CARD_NUMBER = '1232797918'
    AA_CASES_CARD_NUMBER = '1598583432'

    DEATHS_DASHBOARD_URL = 'https://public.domo.com/embed/pages/egBrj'
    DEATHS_CARD_NUMBER = '449723926'
    AA_DEATHS_CARD_NUMBER = '1090276928'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load_response_json(self, webdriver_results, request_key):
        return json.loads(webdriver_results.requests[request_key].response.body.decode('utf8'))

    def extract_cases_rows(self, json_data):
        source_key = pydash.head(list(json_data['chart']['datasources'].keys()))
        return json_data['chart']['datasources'][source_key]['data']['rows']

    def extract_deaths_rows(self, json_data):
        return [d.get('t') for d in json_data['image']['jsonImageMap'] if d.get('t')]

    def get_date(self, soup):
        summary_number = soup.find('summary-number')
        date_string = summary_number.text
        return datetime.strptime(date_string, '%Y-%m-%d').date()

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        cases_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_DASHBOARD_URL)
            .wait_for_number_of_elements((By.XPATH, "//div[@class='badge-content-shield']"), 10)
            .wait_for_presence_of_elements((By.XPATH, '//summary-number/span'))
            .find_request(key='cases', find_by=lambda r: self.CASES_CARD_NUMBER in r.path)
            .find_request(key='cases_by_race', find_by=lambda r: self.AA_CASES_CARD_NUMBER in r.path)
            .get_page_source())

        deaths_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.DEATHS_DASHBOARD_URL)
            .wait_for_number_of_elements((By.XPATH, "//div[@class='kpi_chart']"), 14)
            .find_request(key='deaths', find_by=lambda r: self.DEATHS_CARD_NUMBER in r.path)
            .find_request(key='deaths_by_race', find_by=lambda r: self.AA_DEATHS_CARD_NUMBER in r.path))

        date = self.get_date(cases_results.page_source)

        # total cases
        assert cases_results.requests['cases']
        case_data = self.load_response_json(cases_results, 'cases')
        cases_rows = self.extract_cases_rows(case_data)
        cases = sum(pydash.pluck(cases_rows, 1))

        # aa cases
        assert cases_results.requests['cases_by_race']
        cases_by_race_data = self.load_response_json(cases_results, 'cases_by_race')
        cases_by_race_rows = self.extract_cases_rows(cases_by_race_data)
        aa_row = pydash.find(cases_by_race_rows, lambda r: r[0] == 'Black or African-American')
        assert len(aa_row) == 2, 'Row is malformed'
        aa_cases = aa_row[1]

        # total deaths
        deaths_data = self.load_response_json(deaths_results, 'deaths')
        deaths_rows = self.extract_deaths_rows(deaths_data)
        deaths = sum([raw_string_to_int(s, error='return_default', default=0) for s in deaths_rows])

        # pct_aa_deaths
        deaths_pct_by_race_data = self.load_response_json(deaths_results, 'deaths_by_race')
        deaths_pct_by_race_strings = self.extract_deaths_rows(deaths_pct_by_race_data)
        aa_death_pct_string = pydash.find(deaths_pct_by_race_strings, lambda s: 'Black or African-American' in s)
        pct_pattern = re.compile(r'(\d{1,2}\.\d{2})%')
        matches = pct_pattern.search(aa_death_pct_string)
        assert matches, 'No percentage deaths found.'
        pct_aa_deaths = maybe_convert(matches.group())

        aa_deaths = round((pct_aa_deaths / 100) * deaths)

        pct_aa_cases = to_percentage(aa_cases, cases)

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
