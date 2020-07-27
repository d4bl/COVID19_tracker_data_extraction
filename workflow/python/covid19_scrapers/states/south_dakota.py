from datetime import datetime

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, powerbi
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class SouthDakota(ScraperBase):
    """South Dakota information comes from a PowerBI dashboard.
    The dashboard requests and extracted and parsed with the PowerBI Parser and the needed data is then extracted.

    At the time of writing this, they currently do not report deaths by race.
    """
    URL = 'https://app.powerbigov.us/view?r=eyJrIjoiZWU2Mjc5NDgtMTNkMC00Nzc2LTk1NjktYWFjYzQyZjc5NjMxIiwidCI6IjcwYWY1NDdjLTY5YWItNDE2ZC1iNGE2LTU0M2I1Y2U1MmI5OSJ9&pageName=ReportSectionaa9a40163c6346cbafef'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_date(self, soup):
        last_updated_header = soup.find('div', title='LAST UPDATED')
        date_str = last_updated_header.find_next('title').text
        return datetime.strptime(date_str, '%m/%d/%Y %H:%M:%S %p').date()

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_number_of_elements((By.XPATH, "//*[name()='svg']"), 26)
            .find_request('race_cases', find_by=powerbi.filter_requests(selects=['dashboard.Race/Ethnicity', 'Sum(dashboard.confirmed or probable death)']))
            .find_request('summary_cases', find_by=powerbi.filter_requests(selects=['Custom Totals.Test Results', 'Sum(Custom Totals.# of Cases)']))
            .get_page_source()
        )
        date = self.parse_date(results.page_source)
        parser = powerbi.PowerBIParser(results.requests['summary_cases'])
        df = parser.get_dataframe_by_key('Custom Totals').set_index('Label')
        cases = df.loc['Total Positive Cases*']['Sum(Custom Totals.# of Cases)']
        deaths = df.loc['Deaths***']['Sum(Custom Totals.# of Cases)']

        parser = powerbi.PowerBIParser(results.requests['race_cases'])
        df = parser.get_dataframe_by_key('Race/Ethnicity').set_index('dashboard.Race/Ethnicity')

        # I believe the people who labeled the dashboards labeled them incorrectly
        # the results below actually reflect number of cases and not deaths.
        aa_cases = df.loc['Black, Non-Hispanic']['Sum(dashboard.confirmed or probable death)1']
        known_race_cases = df['Sum(dashboard.confirmed or probable death)1'].sum()

        pct_aa_cases = misc.to_percentage(aa_cases, known_race_cases)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            pct_aa_cases=pct_aa_cases,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_race_cases,
        )]
