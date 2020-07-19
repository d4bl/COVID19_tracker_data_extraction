from datetime import datetime
import re

import pandas as pd
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, parse
from covid19_scrapers.utils.tableau import TableauParser
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class NewHampshire(ScraperBase):
    """New Hampshire data comes from a Tableau dashboard

    The way this is extracted is by going to each of the Tableau tabs.
    When a request is made to the Tableau URL, several requests are made back and forth between the client and server.
    One of the requests made will contain a giant blob that contains 2 pieces of data in json

    The data can then be extracted through a custom parser. From there the needed data can be extracted.
    """
    URL = 'https://nh.gov/t/DHHS/views/COVID-19Dashboard/Summary?%3Aembed=y'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'New Hampshire'

    def get_date(self, soup):
        span = soup.find(text=re.compile(r'data as of', re.I))
        date_str = span.find_next('span')
        return datetime.strptime(date_str.text, '%m/%d/%Y').date()

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()

        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(), 'Race/Ethnicity is known for ')]"))
            .find_request(key='results', find_by=lambda r: 'bootstrapSession' in r.path)
            .get_page_source())

        date = self.get_date(results.page_source)

        resp_body = results.requests['results'].response.body.decode('utf8')
        tableau_parser = TableauParser(resp_body)

        gender_dict = tableau_parser.extract_data_from_key('Summary by Gender')
        df = pd.DataFrame.from_dict(gender_dict).set_index(['Gender', 'Measure Names'])
        cases = parse.raw_string_to_int(df.loc['%all%', 'Cumulative Infections ']['Measure Values'])
        deaths = parse.raw_string_to_int(df.loc['%all%', 'Deaths']['Measure Values'])

        race_dict = tableau_parser.extract_data_from_key('Summary by Race-Eth')
        df = pd.DataFrame.from_dict(race_dict).set_index(['Race/Ethnicity', 'Measure Names'])
        aa_cases = parse.raw_string_to_int(
            df.loc['Black or African American**', 'Cumulative Infections ']['Measure Values'])
        aa_deaths = parse.raw_string_to_int(df.loc['Black or African American**', 'Deaths']['Measure Values'])
        known_race_cases = parse.raw_string_to_int(df.loc['%all%', 'Cumulative Infections ']['Measure Values'])
        known_race_deaths = parse.raw_string_to_int(df.loc['%all%', 'Deaths']['Measure Values'])

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
            pct_includes_hispanic_black=False,
            known_race_cases=known_race_cases,
            known_race_deaths=known_race_deaths
        )]
