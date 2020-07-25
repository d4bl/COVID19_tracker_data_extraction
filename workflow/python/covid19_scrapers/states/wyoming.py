from datetime import datetime

from selenium.webdriver.common.by import By
import pandas as pd
import pydash

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import misc, tableau
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Wyoming(ScraperBase):
    """Data for Wyoming comes from Tableau dashboards. One dashboard contains the cases and cases for ethnicity data
    The other dashboard contains the deaths and deaths for ethnicity data. We extract the data from the requests
    and parse it using the TableauParser.
    """

    CASES_URL = 'https://public.tableau.com/views/EpiCOVIDtest/Dashboard?:embed=y&:showVizHome=no'
    DEATHS_URL = 'https://public.tableau.com/views/EpiCOVIDtest/COVID-19RelatedDeaths?%3Aembed=y&%3AshowVizHome=no'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.CASES_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 58)
            .find_request('cases', find_by=tableau.find_tableau_request)
            .clear_request_history()
            .go_to_url(self.DEATHS_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 29)
            .find_request('deaths', find_by=tableau.find_tableau_request))

        parser = tableau.TableauParser(request=results.requests['cases'])

        raw_date_str = pydash.head(parser.extract_data_from_key('cases')['ATTR(Date Updated)'])
        date = datetime.strptime(raw_date_str, '%A, %B %d, %Y').date()

        confirmed_cases = pydash.head(parser.extract_data_from_key('cases')['SUM(# Lab Confirmed Cases)'])
        probable_cases = pydash.head(parser.extract_data_from_key('probable cases')['SUM(# probable)'])
        cases = confirmed_cases + probable_cases
        cases_df = pd.DataFrame.from_dict(parser.extract_data_from_key('raceth')).set_index('sub-category')
        aa_cases = cases_df.loc['Black']['SUM(count)']
        known_race_cases = cases - cases_df.loc['unknown']['SUM(count)']

        parser = tableau.TableauParser(request=results.requests['deaths'])
        deaths = pydash.head(parser.extract_data_from_key('death (2)')['SUM(# lab confirmed deaths)'])
        deaths_df = pd.DataFrame.from_dict(parser.extract_data_from_key('raceth (death)')).set_index('sub-category')
        deaths_df = deaths_df.assign(Count=[round(v * deaths) for v in deaths_df['SUM(% of deaths)'].values])
        aa_deaths = deaths_df.loc['Black']['Count']
        known_race_deaths = deaths - deaths_df.loc['unknown']['Count']

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
            known_race_deaths=known_race_deaths,
        )]
