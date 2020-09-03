from datetime import datetime

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, powerbi
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class WestVirginia(ScraperBase):
    """West Virginia information comes from a PowerBI dashboard.
    The dashboard requests and extracted and parsed with the PowerBI Parser and the needed data is then extracted.

    At the time of writing this, deaths by race seems unavailable.
    """
    URL = 'https://app.powerbigov.us/view?r=eyJrIjoiYmE2MGRlZTAtNWNmMi00NDY5LTg2MWUtYzgwYjZlZjZkM2MxIiwidCI6IjhhMjZjZjAyLTQzNGEtNDMxZS04Y2FkLTdlYWVmOTdlZjQ4NCJ9'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .set_request_capture_scope(['.*querydata?synchronous=true'])
            .go_to_url(self.URL)
            .wait_for_visibility_of_elements((By.XPATH, "//span[contains(text(), 'Cumulative Summary')]/parent::*"))
            .wait_for_number_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='text']/*[name()='title']"), 7)
            .find_request('summary', find_by=powerbi.filter_requests(entity='factCase Data', selects=['Case Data.Total Cases']))
            .find_request('deaths', find_by=powerbi.filter_requests(entity='factCase Data', selects=['Sum(Case Data.Death)']))
            .find_request('last_updated', find_by=powerbi.filter_requests(entity='Today Dates', selects=['Min(Today Dates.Today)']))
            .find_element_by_xpath("//span[contains(text(), 'Cumulative Summary')]/parent::*")
            .clear_request_history()
            .click_on_last_element_found()
            .wait_for_number_of_elements((By.XPATH, "//*[name()='svg']/*[name()='g']/*[name()='text']/*[name()='title']"), 25)
            .find_request('race_cases', find_by=powerbi.filter_requests(selects=['Case Data.Race Group']))
        )

        parser = powerbi.PowerBIParser(request=results.requests['last_updated'])
        date_df = parser.get_dataframe_by_key('Date')
        timestamp = date_df.loc[0]['Min(Today Dates.Today)'] / 1000  # convert to seconds
        date = datetime.fromtimestamp(timestamp).date()

        parser = powerbi.PowerBIParser(request=results.requests['summary'])
        cases_df = parser.get_dataframe_by_key('Total Cases')
        cases = cases_df.loc[0]['Case Data.Total Cases']

        parser = powerbi.PowerBIParser(request=results.requests['deaths'])
        deaths_df = parser.get_dataframe_by_key('Death')
        deaths = deaths_df.loc[0]['Sum(Case Data.Death)']

        parser = powerbi.PowerBIParser(request=results.requests['race_cases'])
        race_cases_df = parser.get_dataframe_by_key('Race').set_index('Case Data.Race Group')
        aa_cases = race_cases_df.loc['Black']['Case Data.Total Cases']
        known_race_cases = race_cases_df['Case Data.Total Cases'].sum()

        pct_aa_cases = misc.to_percentage(aa_cases, known_race_cases)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            pct_aa_cases=pct_aa_cases,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
