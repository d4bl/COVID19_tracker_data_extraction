from datetime import datetime
import re

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, powerbi
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Nevada(ScraperBase):
    """Data for Nevada comes from a PowerBI dashboards located on 2 seperate pages

    We use the webdriver to go to the individual pages, parse the necessary responses with the PowerBIScraper and extract the results.
    """
    URL = 'https://app.powerbigov.us/view?r=eyJrIjoiMjA2ZThiOWUtM2FlNS00MGY5LWFmYjUtNmQwNTQ3Nzg5N2I2IiwidCI6ImU0YTM0MGU2LWI4OWUtNGU2OC04ZWFhLTE1NDRkMjcwMzk4MCJ9'

    @property
    def race_url(self):
        return self.URL + '&pageName=ReportSection1fd478b19ea7b4923700'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_date(self, soup):
        date_element = soup.find('span', text=re.compile('Last updated'))
        match = re.search(r'\d{1,2}\/\d{1,2}\/\d{4}', date_element.text)
        return datetime.strptime(match.group(), '%m/%d/%Y').date()

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_number_of_elements((By.XPATH, '//visual-modern'), 34)
            .wait_for_presence_of_elements((By.XPATH, '//span[contains(text(), "Last updated")]'))
            .find_request('summary', find_by=powerbi.filter_requests(entity='Trend Analysis', selects=['Sum(County.Deaths)']))
            .get_page_source()
            .clear_request_history()
            .go_to_url(self.race_url)
            .wait_for_presence_of_elements((By.XPATH, "//*[name()='title' and contains(text(), 'Black')]"))
            .wait_for_presence_of_elements((By.XPATH, "//*[name()='title' and contains(text(), 'Asian')]"))
            .find_request('race_cases', find_by=powerbi.filter_requests(entity='Demographics', selects=['CountNonNull(Demographics.Black)']))
            .find_element_by_xpath("//div[@class='slicer-restatement']")
            .click_on_last_element_found()
            .wait_for_visibility_of_elements((By.XPATH, "//div[@class='row' and .//span/@title='Deaths']"))
            .find_element_by_xpath("//div[@class='row' and .//span/@title='Deaths']")
            .clear_request_history()
            .click_on_last_element_found()
            .switch_to_iframe()
            .wait_for_presence_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='g']/*[name()='text']/*[name()='title' and contains(text(), 'deaths')]"))
            .find_request('race_deaths', find_by=powerbi.filter_requests(entity='Demographics', selects=['CountNonNull(Demographics.Black)']))
        )

        page_source = results.page_source
        date = self.get_date(page_source)

        parser = powerbi.PowerBIParser(results.requests['summary'])
        cases_df = parser.get_dataframe_by_key('Sum(County.Total Cases)')
        cases = cases_df.loc[0, 'Sum(County.Total Cases)']
        deaths_df = parser.get_dataframe_by_key('Sum(County.Deaths)')
        deaths = deaths_df.loc[0, 'Sum(County.Deaths)']

        parser = powerbi.PowerBIParser(results.requests['race_cases'])
        race_cases_df = parser.get_dataframe_by_key('Demographics')
        aa_cases = race_cases_df.loc[0, 'CountNonNull(Demographics.Black)']
        known_race_cases = race_cases_df.loc[0].sum()

        parser = powerbi.PowerBIParser(results.requests['race_deaths'])
        race_deaths_df = parser.get_dataframe_by_key('Demographics')
        aa_deaths = race_deaths_df.loc[0, 'CountNonNull(Demographics.Black)']
        known_race_deaths = race_deaths_df.loc[0].sum()

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
