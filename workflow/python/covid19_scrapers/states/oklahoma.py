import logging
import pandas as pd
from selenium.webdriver.common.by import By
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.webdriver import WebdriverSteps, WebdriverRunner
from re import search

_logger = logging.getLogger(__name__)


class Oklahoma(ScraperBase):
    """Oklahoma provides the CSV version of the total cases
    and a Looker dashboard with the percentage of cases by race.
    The COVID-19 datasets are at
    https://coronavirus.health.ok.gov/
    """
    CASES_DASHBOARD_OK = 'https://looker-dashboards.ok.gov/embed/dashboards/75'
    DEATH_DASHBOARD_OK = 'https://looker-dashboards.ok.gov/embed/dashboards/76'
    CASES_CSV_URL = 'https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_city.csv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # For statewide totals, sum the latest city figures.
        total_df = pd.read_csv(
            self.CASES_CSV_URL,
            parse_dates=True
        ).groupby('ReportDate').sum().sort_index(ascending=False)
        total_cases = int(total_df.iloc[0]['Cases'])
        total_deaths = int(total_df.iloc[0]['Deaths'])
        date = total_df.iloc[0]['ReportDate']

        # OK demographic breakdowns have to be obtained through WebdriverRunner
        # and by scraping the Looker dashboard
        runner = WebdriverRunner()
        cases_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_DASHBOARD_OK)
            .wait_for_presence_of_elements([(By.XPATH, "//a[@target='_self']")])
            .get_page_source())

        runner = WebdriverRunner()
        death_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.DEATH_DASHBOARD_OK)
            .wait_for_presence_of_elements([(By.XPATH, "//a[@target='_self']")])
            .get_page_source())

        # Once we have the page source for both dashboard, I'm extracting the "tspan"
        # tags which include the percentages for black lives
        cases_results_list = [values.text.strip().replace('%', '') for values in cases_results.page_source.find_all('tspan')]
        death_results_list = [values.text.strip().replace('%', '') for values in death_results.page_source.find_all('tspan')]

        for i in cases_results_list:
            if search('Unknown', i):
                string_index = cases_results_list.index(i)
                percentage_unkown_cases = float(cases_results_list[string_index + 1])
            elif search('African', i):
                string_index = cases_results_list.index(i)
                aa_cases_pct = float(cases_results_list[string_index + 2])

        for i in death_results_list:
            if search('Unknown', i):
                string_index = death_results_list.index(i)
                percentage_unkown_deaths = death_results_list[string_index + 1]
            elif search('African', i):
                string_index = death_results_list.index(i)
                aa_deaths_pct = death_results_list[string_index + 2]

        aa_cases = (aa_cases_pct / 100) * total_cases
        aa_deaths = (aa_deaths_pct / 100) * total_deaths

        known_cases = total_cases * (1 - (percentage_unkown_cases / 100))
        known_deaths = total_deaths * (1 - (percentage_unkown_deaths / 100))

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
