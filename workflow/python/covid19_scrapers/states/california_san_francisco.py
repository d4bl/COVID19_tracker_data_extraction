from datetime import datetime
import re

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, powerbi
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class CaliforniaSanFrancisco(ScraperBase):
    """The San Francisco data comes from a PowerBI dashboard

    Request/Responses from PowerBI can be searched for and found such that
    the needed data can be extracted. This is done so with the `PowerBIParser` util
    """
    URL = 'https://app.powerbigov.us/view?r=eyJrIjoiMWEyYTRhYTAtMTdjYi00YTE1LWJiMTQtYTY3NmJmMjJhOThkIiwidCI6IjIyZDVjMmNmLWNlM2UtNDQzZC05YTdmLWRmY2MwMjMxZjczZiJ9'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'California - San Francisco'
    
    @classmethod
    def is_beta(cls):
        return getattr(cls, 'BETA_SCRAPER', True)

    def parse_date(self, date_string):
        pattern = re.compile(r'\d+\/\d+\/\d+')
        results = pattern.search(str(date_string))
        assert results, 'No date found'
        parsed_date = results.group()
        return datetime.strptime(parsed_date, '%m/%d/%Y').date()

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_presence_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='g']/*[name()='g']/*[name()='text']/*[name()='title' and contains(text(), 'Black')]"))
            .wait_for_visibility_of_elements((By.XPATH, "//button[span[contains(text(), 'Deaths')]]"))
            .find_request('date', find_by=powerbi.filter_requests(entity='Date_Uploaded'))
            .find_request('cases_by_race', find_by=powerbi.filter_requests(entity='Cases_Ethnicity'))
            .find_element_by_xpath("//button[span[contains(text(), 'Deaths')]]")
            .clear_request_history()
            .click_on_last_element_found()
            .wait_for_number_of_elements((By.XPATH, "//div[contains(@aria-label, 'Deaths -')]"), 6)
            .wait_for_number_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='rect']"), 20)
            .find_request('deaths_by_race', find_by=powerbi.filter_requests(entity='Deaths_Ethnicity')))

        assert 'date' in results.requests, '`date` request missing'
        assert 'cases_by_race' in results.requests, '`cases_by_race` request missing'
        assert 'deaths_by_race' in results.requests, '`deaths_by_race` request missing'

        # Date
        parser = powerbi.PowerBIParser(results.requests['date'])
        df = parser.get_dataframe_by_key('Date_Uploaded')
        unparsed_date = df.loc[0]['Date_Uploaded.Data as of']
        date = self.parse_date(unparsed_date)

        # Cases
        parser = powerbi.PowerBIParser(results.requests['cases_by_race'])
        cases_by_race_df = parser.get_dataframe_by_key('Cases_Ethnicity').set_index('CountNonNull(Cases_Ethnicity.Total Cases)')
        cases = cases_by_race_df['Cases_Ethnicity.raceethnicity'].sum()
        aa_cases = cases_by_race_df.loc['Black or African American']['Cases_Ethnicity.raceethnicity']
        known_cases = cases - cases_by_race_df.loc['Unknown']['Cases_Ethnicity.raceethnicity']
        pct_aa_cases = misc.to_percentage(aa_cases, known_cases)

        # Deaths
        parser = powerbi.PowerBIParser(results.requests['deaths_by_race'])
        deaths_by_race_df = (parser.get_dataframe_by_key(key='Deaths_Ethnicity')
                             .set_index('Sum(Deaths_Ethnicity.Total Cases)'))
        deaths = deaths_by_race_df['Deaths_Ethnicity.raceethnicity'].sum()
        aa_deaths = deaths_by_race_df.loc['Black or African American']['Deaths_Ethnicity.raceethnicity']
        # if there are no unknown deaths, it will not appear in the dataframe.
        known_deaths = deaths
        if 'Unknown' in deaths_by_race_df.index:
            known_deaths -= deaths_by_race_df.loc['Unknown']['Deaths_Ethnicity.raceethnicity']
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
