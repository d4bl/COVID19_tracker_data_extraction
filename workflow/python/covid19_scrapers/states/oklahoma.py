import logging
import pandas as pd
from selenium.webdriver.common.by import By
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.webdriver import WebdriverSteps, WebdriverRunner

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
            .wait_for_number_of_elements((By.XPATH, "//a[@target='_self']"), 6)
            .get_page_source())

        death_results = runner.run(
            WebdriverSteps()
            .go_to_url(self.DEATH_DASHBOARD_OK)
            .wait_for_number_of_elements((By.XPATH, "//a[@target='_self']"), 6)
            .get_page_source())

        # Once we have the page source for both dashboard, I'm extracting the "tspan"
        # tags which include the percentages for black lives
        cases_percentages_results = [percentages.text.strip()
                                     for percentages in cases_results.page_source.find_all('tspan')]

        death_percentages_results = [percentages.text.strip()
                                     for percentages in death_results.page_source.find_all('tspan')]

        aa_cases_pct = float(cases_percentages_results[6].strip('%'))
        aa_deaths_pct = float(death_percentages_results[6].strip('%'))

        aa_cases = aa_cases_pct * (total_cases / 100)
        aa_deaths = aa_deaths_pct * (total_deaths / 100)

        # Acquiring percentage of unkown cases & deaths
        percentage_unkown_cases = float(cases_percentages_results[2].strip('%'))
        percentage_unkown_deaths = float(death_percentages_results[2].strip('%'))

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
