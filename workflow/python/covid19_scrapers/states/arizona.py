from datetime import datetime

import pydash
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc
from covid19_scrapers.utils.tableau import TableauParser
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Arizona(ScraperBase):
    """Arizona data is extracted from two Tableau dashboards: cases and deaths.

    The way this is extracted is by going to each of the Tableau links.
    When a request is made to the Tableau URL, several requests are made back and forth between the client and server.
    One of the requests made will contain a giant blob that contains 2 pieces of data in json

    The data can then be extracted through a custom parser. From there the needed data can be extracted.
    """
    CASES_URL = 'https://tableau.azdhs.gov/views/COVID19Demographics/EpiData?:embed=y&:showVizHome=y'
    DEATHS_URL = 'https://tableau.azdhs.gov/views/COVID-19Deaths/Deaths?:embed=y&:showVizHome=y'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        cases_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(), 'COVID-19 Cases by Race/Ethnicity')]"))
            .find_request(key='cases', find_by=lambda r: 'bootstrapSession' in r.path))

        deaths_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.DEATHS_URL)
            .wait_for_presence_of_elements((By.XPATH, "//span[contains(text(), 'COVID-19 Deaths by Race/Ethnicity')]"))
            .find_request(key='deaths', find_by=lambda r: 'bootstrapSession' in r.path))

        assert cases_results.requests['cases'], 'No results found for `cases`'
        resp_body = cases_results.requests['cases'].response.body.decode('utf8')
        tableau_parser = TableauParser(resp_body)

        parsed_date = tableau_parser.extract_data_from_key('Date Updated')
        assert 'Date Updated' in parsed_date, 'Unable to parse date'
        assert len(parsed_date['Date Updated']) == 1, 'Unable to parse date'
        date_str = pydash.head(parsed_date['Date Updated'])
        date = datetime.strptime(date_str, '%m/%d/%Y').date()

        parsed_num_cases = tableau_parser.extract_data_from_key('Number of Cases')
        assert 'SUM(Number of Records)' in parsed_num_cases, 'Key not found, unable to parse number of records'
        assert len(parsed_num_cases['SUM(Number of Records)']) == 1, 'Parsing error might have occurred'
        cases = pydash.head(parsed_num_cases['SUM(Number of Records)'])

        parsed_race_eth = tableau_parser.extract_data_from_key('Race/Ethnicity Epi')
        assert 'Raceeth' in parsed_race_eth, 'Missing key in parsed_race_eth'
        aa_cases_idx = parsed_race_eth['Raceeth'].index('Black, non-Hispanic')
        assert 'AGG(RecordCount)' in parsed_race_eth, 'Missing total AA cases'
        aa_cases = parsed_race_eth['AGG(RecordCount)'][aa_cases_idx]

        assert deaths_results.requests['deaths'], 'No results found for `deaths`'
        resp_body = deaths_results.requests['deaths'].response.body.decode('utf8')
        tableau_parser = TableauParser(resp_body)
        parsed_death_cases = tableau_parser.extract_data_from_key('Number of deaths')
        parsed_deaths_by_race = tableau_parser.extract_data_from_key('Death Race/Ethnicity')
        assert 'SUM(Death count)' in parsed_death_cases, 'Death count not found'
        assert len(parsed_death_cases['SUM(Death count)']) == 1, 'Parsing error might have occurred.'
        deaths = pydash.head(parsed_death_cases['SUM(Death count)'])

        assert 'Raceeth' in parsed_deaths_by_race, 'Missing key in parsed_deaths_by_race'
        aa_deaths_idx = parsed_deaths_by_race['Raceeth'].index('Black, non-Hispanic')
        assert 'AGG(RecordCount)' in parsed_deaths_by_race, 'Missing total AA deaths'
        aa_deaths = parsed_deaths_by_race['AGG(RecordCount)'][aa_deaths_idx]

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
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
