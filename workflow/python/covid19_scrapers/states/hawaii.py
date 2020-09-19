from datetime import datetime
import re

from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import arcgis, misc, tableau
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Hawaii(ScraperBase):
    """The data from Hawaii comes from 3 sources:
        date: the main page
        cases and deaths: arcgis api
        cases by race: tableau dashboard.

    At the time of implementation, Hawaii does not report deaths by race data.
    """
    SUMMARY_QUERY = dict(
        flc_id='20126c66ea9c479f9a4279722f418f05',
        layer_name='covid_county_counts',
        stats=[
            arcgis.make_geoservice_stat('sum', 'cases', 'Cases'),
            arcgis.make_geoservice_stat('sum', 'deaths', 'Deaths')
        ]
    )

    RACE_URL = 'https://public.tableau.com/views/HawaiiCOVID-19-RaceChart/ChartDash?:showVizHome=no'
    MAIN_PAGE_URL = 'https://health.hawaii.gov/coronavirusdisease2019/what-you-should-know/current-situation-in-hawaii/'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_date(self, soup):
        elt = soup.find(string=re.compile('Updated daily')).parent
        match = re.search(r'\w+ \d{1,2}, \d{2,4}', elt.text)
        return datetime.strptime(match.group(), '%B %d, %Y').date()

    def _scrape(self, **kwargs):
        _, summary_df = arcgis.query_geoservice(**self.SUMMARY_QUERY)
        cases = summary_df.loc[0, 'Cases']
        deaths = summary_df.loc[0, 'Deaths']

        runner = WebdriverRunner()
        results = runner.run(
            WebdriverSteps()
            .go_to_url(self.RACE_URL)
            .wait_for_number_of_elements((By.XPATH, '//canvas'), 14)
            .find_request('race_cases', find_by=tableau.find_tableau_request)
            .go_to_url(self.MAIN_PAGE_URL)
            .get_page_source()
        )
        soup = results.page_source
        date = self.get_date(soup)

        parser = tableau.TableauParser(request=results.requests['race_cases'])
        cases_df = parser.get_dataframe_from_key('Census')
        cases_df = cases_df[cases_df['Measure Names'] == 'Case %'].set_index('Race')
        aa_cases = cases_df.loc['Black', 'SUM(Case Count)']
        known_race_cases = cases_df['SUM(Case Count)'].sum()

        pct_aa_cases = misc.to_percentage(aa_cases, known_race_cases)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            pct_aa_cases=pct_aa_cases,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_race_cases,
        )]
