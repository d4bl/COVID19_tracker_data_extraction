import logging
from selenium.webdriver.common.by import By
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.webdriver import WebdriverSteps, WebdriverRunner
import re

_logger = logging.getLogger(__name__)


class Oklahoma(ScraperBase):
    """Oklahoma provides the CSV version of the total cases
    and a Looker dashboard with the percentage of cases by race.
    The COVID-19 datasets are at
    https://coronavirus.health.ok.gov/
    """

    OVERALL_DASHBOARD = 'https://looker-dashboards.ok.gov/embed/dashboards-next/40'
    CASES_DASHBOARD_OK = 'https://looker-dashboards.ok.gov/embed/dashboards/75'
    DEATH_DASHBOARD_OK = 'https://looker-dashboards.ok.gov/embed/dashboards/76'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):

        # Pulling date, total cases, and deaths.
        runner = WebdriverRunner()
        cases_results_dashboard = runner.run(
            WebdriverSteps()
            .go_to_url(self.OVERALL_DASHBOARD)
            .wait_for_presence_of_elements([(By.XPATH, "//a[@target='_self']")])
            .get_page_source())

        date = cases_results_dashboard.page_source.find(text='OK Summary').findNext('a').text
        total_cases = int(cases_results_dashboard.page_source.find(text='OK Cases').findNext('a').text.replace(',', ''))
        total_deaths = int(cases_results_dashboard.page_source.find(text='OK Deaths').findNext('a').text.replace(',', ''))

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
        def _get_demographic_data(page):
            by_race_title = page.find(text=re.compile(r'by race', re.I))
            by_race_svg = by_race_title.find_next('svg')
            legend = by_race_svg.find('g', class_='highcharts-legend')
            pct_re = re.compile(r'([0-9.]+)%')
            for legend_item in legend.find_all('g', class_='highcharts-legend-item'):
                text = ' '.join((tspan.text.strip() for tspan in legend_item.find_all('tspan')))
                if text.find('Unknown') >= 0:
                    match = pct_re.search(text)
                    assert match is not None, 'Unable to find value for label "Unknown"'
                    unknown_pct = float(match.group(1))
                elif text.find('African') >= 0:
                    match = pct_re.search(text)
                    assert match is not None, 'Unable to find value for label "African"'
                    aa_pct = float(match.group(1))
            return (unknown_pct, aa_pct)

        percentage_unkown_cases, aa_cases_pct = _get_demographic_data(cases_results.page_source)
        percentage_unkown_deaths, aa_deaths_pct = _get_demographic_data(death_results.page_source)

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
