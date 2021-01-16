from datetime import datetime
from functools import partial

import pandas as pd
import pydash
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, parse, tableau
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class NewYork(ScraperBase):
    """
    NY Data is reported using Tableau, at the time of writing this cases by race is not reported.

    The Fatality Race information however, only reports NY State excluding NYC.
    As a result, we need to pull NYC info seperately and add it to the information in NYS
    """
    SUMMARY_URL = 'https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker/NYSDOHCOVID-19Tracker-TableView?%3Aembed=yes&%3Atoolbar=no&%3Atabs=n'
    DEATHS_URL = 'https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker/NYSDOHCOVID-19Tracker-Fatalities?%3Aembed=yes&%3Atoolbar=no&%3Atabs=n'
    NYS_RACE_DEATHS_URL = 'https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker/NYSDOHCOVID-19Tracker-FatalityDetail?%3Aembed=yes&%3Atoolbar=no&%3Atabs=n'
    NYC_RACE_DEATHS_URL = 'https://raw.githubusercontent.com/nychealth/coronavirus-data/master/totals/by-race.csv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_cases_df(self, data):
        df = pd.DataFrame.from_dict(data).set_index('County')
        df = df[df['Measure Names'] == 'Total Tested Positive']
        df['Measure Values'] = df['Measure Values'].apply(
            partial(parse.raw_string_to_int, error='return_default', default=0))
        return df

    def get_deaths_df(self, data):
        df = pd.DataFrame.from_dict(data).set_index('County')
        df = df[df['Measure Names'] == 'Place of Fatality']
        df['Measure Values'] = df['Measure Values'].apply(
            partial(parse.raw_string_to_int, error='return_default', default=0))
        return df

    def get_nys_race_deaths_df(self, data):
        df = pd.DataFrame.from_dict(data).set_index('Race/Ethnicity')
        df = df[df['Measure Names'] == 'Fatality Count']
        df['Measure Values'] = df['Measure Values'].apply(
            partial(parse.raw_string_to_int, error='return_default', default=0))
        return df

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.SUMMARY_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 12)
            .find_request('summary', find_by=tableau.find_tableau_request)
            .clear_request_history()
            .go_to_url(self.DEATHS_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 31)
            .find_request('deaths', find_by=tableau.find_tableau_request)
            .clear_request_history()
            .go_to_url(self.NYS_RACE_DEATHS_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 20)
            .find_request('race_deaths', find_by=tableau.find_tableau_request))

        parser = tableau.TableauParser(request=results.requests['summary'])
        date_info = parser.extract_data_from_key('DASHBOARD 1 DATE (2)')
        date_str = pydash.get(date_info, 'MAX(Last_Reported_Test_Formatted).0')
        date = datetime.strptime(date_str, '%m/%d/%Y').date()

        cases_df = self.get_cases_df(parser.extract_data_from_key('TABLE VIEW (LARGE)'))
        cases = cases_df.loc['%all%']['Measure Values']

        parser = tableau.TableauParser(request=results.requests['deaths'])
        deaths_df = self.get_deaths_df(parser.extract_data_from_key('Fatalaties by County'))
        deaths = deaths_df.loc['%all%']['Measure Values']

        parser = tableau.TableauParser(request=results.requests['race_deaths'])
        nys_race_deaths_df = self.get_nys_race_deaths_df(parser.extract_data_from_key('Race/Ethnicity Table (2)'))
        nyc_race_deaths_df = pd.read_csv(self.NYC_RACE_DEATHS_URL).set_index('RACE_GROUP')
        aa_deaths = (nys_race_deaths_df.loc['Black']['Measure Values']
                     + nyc_race_deaths_df.loc['Black/African-American']['DEATH_COUNT'])
        known_race_deaths = nys_race_deaths_df.loc['Total']['Measure Values'] + nyc_race_deaths_df['DEATH_COUNT'].sum()

        pct_aa_deaths = misc.to_percentage(aa_deaths, known_race_deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_deaths=aa_deaths,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_deaths=known_race_deaths
        )]
