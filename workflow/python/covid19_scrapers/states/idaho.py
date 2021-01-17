from datetime import datetime
import re

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import tableau
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Idaho(ScraperBase):
    """Scraper for Idaho which is obtained via data from a Tableau dashboard.

    Date is obtained from their homepage where the dashboard is embedded.
    Separate requests are then made out to the individual Tableau dashboards
    where the demographic data is obtained.
    """
    HOME_PAGE_URL = 'https://coronavirus.idaho.gov/'
    DEMOGRAPHIC_CASES_URL = 'https://public.tableau.com/profile/idaho.division.of.public.health#!/vizhome/DPHIdahoCOVID-19Dashboard/Demographics'
    DEMOGRAPHIC_DEATHS_URL = 'https://public.tableau.com/profile/idaho.division.of.public.health#!/vizhome/DPHIdahoCOVID-19Dashboard/DeathDemographics'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.HOME_PAGE_URL)
            .get_page_source()
            .go_to_url(self.DEMOGRAPHIC_CASES_URL)
            .find_request(key='cases', find_by=tableau.find_tableau_request)
            .clear_request_history()
            .go_to_url(self.DEMOGRAPHIC_DEATHS_URL)
            .find_request('deaths', find_by=tableau.find_tableau_request)
        )

        date_str_element = results.page_source.find('strong', string=re.compile('current'))
        assert date_str_element, 'No date element found'
        date_str = date_str_element.get_text()
        pattern = re.compile(r'(\d{1,2}\/\d{1,2}\/\d{4})')
        matches = pattern.search(date_str)
        assert matches, 'Date not found.'
        date = datetime.strptime(matches.group(), '%m/%d/%Y').date()

        parser = tableau.TableauParser(request=results.requests['cases'])
        cases_df = parser.get_dataframe_from_key('CaseRace').set_index('Measure Status')
        cases = cases_df.loc['Black or African American']['AGG(Calculation1)']
        aa_cases = round(cases_df.loc['Black or African American']['SUM(Count)'] * cases)

        parser = tableau.TableauParser(request=results.requests['deaths'])
        deaths = parser.get_dataframe_from_key('Total Deaths (2)')['SUM(Deaths)'].sum()
        deaths_df = parser.get_dataframe_from_key('Race').set_index('Measure Status11')
        aa_deaths = round(deaths_df.loc['Black or African American']['SUM(Deaths)'] * deaths)

        pct_aa_cases = to_percentage(aa_cases, cases)
        pct_aa_deaths = to_percentage(aa_deaths, deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
        )]
