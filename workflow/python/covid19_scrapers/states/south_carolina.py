import pandas as pd
import pydash
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, tableau
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class SouthCarolina(ScraperBase):
    """South Carolina data comes from a tableau dashboard

    We search the request for the Tableau data, parse it and extract the needed info.
    """

    URL = 'https://public.tableau.com/views/EpiProfile/DemoStory?:embed=y&:showVizHome=no'

    def __init__(self, **kwargs):
        # super().__init__(**kwargs)
        pass

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 32)
            .find_request('cases', tableau.find_tableau_request)
            .clear_request_history()
            .find_element_by_xpath('//*[@id="tabZoneId4"]/div/div/div/span[2]/div/span/span/span[2]/div[2]/div')
            .click_on_last_element_found()
            .wait_for_number_of_elements((By.XPATH, "//span[contains(text(), 'Deaths')]"), 6)
            .find_request('deaths', find_by=lambda r: 'set-active-story-point' in r.path)
        )

        parser = tableau.TableauParser(request=results.requests['cases'])
        cases = pydash.head(parser.extract_data_from_key('Cases')['SUM(Number of Records)'])
        cases_pct_df = pd.DataFrame.from_dict(parser.extract_data_from_key('Race_Cases')).set_index('Assigned_Race')
        cases_df = cases_pct_df.assign(Count=[round(v * cases) for v in cases_pct_df['SUM(Number of Records)'].values])
        aa_cases = cases_df.loc['Black']['Count']
        known_race_cases = cases - cases_df.loc['Unknown']['Count']

        parser.update_with_vql_command_request(request=results.requests['deaths'])

        deaths = pydash.head(parser.extract_data_from_key('NumberDeaths')['SUM(Number of Records)'])
        deaths_pct_df = pd.DataFrame.from_dict(parser.extract_data_from_key('Race_Deaths')).set_index('Assigned_Race')
        deaths_df = deaths_pct_df.assign(Count=[round(v * deaths) for v in deaths_pct_df['SUM(Number of Records)'].values])
        aa_deaths = deaths_df.loc['Black']['Count']
        known_race_deaths = deaths - deaths_df.loc['Unknown']['Count']

        pct_aa_cases = misc.to_percentage(aa_cases, known_race_cases)
        pct_aa_deaths = misc.to_percentage(aa_deaths, known_race_deaths)

        return [self._make_series(
            date=None,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_race_cases,
            known_race_deaths=known_race_deaths
        )]
