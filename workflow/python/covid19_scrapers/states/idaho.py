from datetime import datetime, timedelta

import pandas as pd
from pytz import timezone
from selenium.webdriver.common.by import By

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.http import get_content_as_file
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.webdriver import WebdriverRunner, WebdriverSteps


class Idaho(ScraperBase):
    """Scraper for Idaho which is obtained via data from a Tableau dashboard.

    The method of scraping is the same as North Carolina, see `covid19_scrapers/states/north_carolina.py::get_demographic_dataframe` for details

    Via the demographics deaths data, the sum of all the races correctly add up to the reported total.
    However for the cases data, the sum of all the races do not correctly add up to the total,
    which is why a seperate request is made to obtain the total value.
    """
    BASE_URL = (
        'https://public.tableau.com/views/DPHIdahoCOVID-19Dashboard_V2/Story1?'
        '%3Aembed=y&%3AshowVizHome=no&%3Adisplay_count=y&%3Adisplay_static_image=y&%3AbootstrapWhenNotified=true'
        "&%3Alanguage=en&:embed=y&:showVizHome=n&:apiID=host0#navType=0&navSrc=Parse'")

    TOTAL_CASES_DATA_URL = (
        'https://public.tableau.com/vizql/w/DPHIdahoCOVID-19Dashboard_V2/v/Story1/viewData/'
        'sessions/{}/views/2142284533943777519_13019088007435048908'
        '?maxrows=200&viz=%7B%22worksheet%22%3A%22State%20Total%20Cases%20Display%20(2)%22%2C'
        '%22dashboard%22%3A%22DPH%20COVID19%20State%22%2C%22storyboard'
        '%22%3A%22Story%201%22%2C%22storyPointId%22%3A3%7D')

    DEMOGRAPHIC_CASES_DATA_URL = (
        'https://public.tableau.com/vizql/w/DPHIdahoCOVID-19Dashboard_V2/v/Story1/viewData/'
        'sessions/{}/views/11831741491762752444_4886583326715823757'
        '?maxrows=200&viz=%7B%22worksheet%22%3A%22CaseRace%22%2C'
        '%22dashboard%22%3A%22DPH%20COVID19%20State%20DEMO%22%2C%22storyboard'
        '%22%3A%22Story%201%22%2C%22storyPointId%22%3A5%7D')

    DEMOGRAPHIC_DEATHS_DATA_URL = (
        'https://public.tableau.com/vizql/w/DPHIdahoCOVID-19Dashboard_V2/v/Story1/viewData/'
        'sessions/{}/views/13810090252421852225_10680656179405171816'
        '?maxrows=200&viz=%7B%22worksheet%22%3A%22Race%22%2C'
        '%22dashboard%22%3A%22Table%20Dashboard%20(w%2Fdeath%20place)%22%2C%22storyboard'
        '%22%3A%22Story%201%22%2C%22storyPointId%22%3A9%7D')

    STATEWIDE_DEMOGRAPHICS_TAB_TEXT = 'Statewide Demographics'
    STATEWIDE_DEMOGRAPHICS_DASHBOARD_TITLE = 'COVID-19 Demographics'

    DEATH_DEMOGRAPHICS_TAB_TEXT = 'Deaths Demographics'
    DEATH_DEMOGRAPHICS_DASHBOARD_TITLE = 'Idaho Resident COVID-19 Related Deaths'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _df_from_url(self, url, index_col=None):
        runner = WebdriverRunner()
        results = runner.run(WebdriverSteps()
                             .go_to_url(url)
                             .wait_for([(By.XPATH, "//div[@id='tabBootErrTitle' and contains(text(),'Unexpected Error')]")])
                             .go_to_url(url)
                             .wait_for([(By.CLASS_NAME, 'csvLink_summary')])
                             .get_page_source())
        soup = results.page_source
        link = soup.find('a', {'class': 'csvLink_summary'})
        assert link, 'No CSV link found'
        csv_href = link.get('href')
        assert csv_href, 'No CSV link found'
        content = get_content_as_file(csv_href)
        return pd.read_csv(content, index_col=index_col)

    def get_date(self):
        # No reliable date could be found. However, dashboard states that
        # the data updates at 5pm (US/Mountain)
        # so check for current US/Mountain time. If it is past 5pm, use the
        # current date, otherwise use yesterday's date.
        now = datetime.now(timezone('US/Mountain'))
        return (now.date() - timedelta(days=1)
                if now.hour < 5 + 12  # add 12 hours to switch to military time
                else now.date())

    def setup_session(self):
        runner = WebdriverRunner()
        return runner.run(
            WebdriverSteps()
            .go_to_url(self.BASE_URL)
            .wait_for([(By.ID, 'dashboard-viewport')])
            .get_x_session_id()
            .wait_for([
                (By.CLASS_NAME, 'tabStoryPointContent'),
                (By.CLASS_NAME, 'tab-widget')])
            .find_element_by_xpath(f"//*[contains(text(), '{self.STATEWIDE_DEMOGRAPHICS_TAB_TEXT}')]")
            .click_on_last_element_found()
            .wait_for([(By.XPATH, f"//*[contains(text(), '{self.STATEWIDE_DEMOGRAPHICS_DASHBOARD_TITLE}')]")])
            .find_element_by_xpath(f"//*[contains(text(), '{self.DEATH_DEMOGRAPHICS_TAB_TEXT}')]")
            .click_on_last_element_found()
            .wait_for([(By.XPATH, f"//*[contains(text(), '{self.DEATH_DEMOGRAPHICS_DASHBOARD_TITLE}')]")]))

    def get_total_cases(self, x_session_id):
        df = self._df_from_url(self.TOTAL_CASES_DATA_URL.format(x_session_id))
        assert len(df) == 1, 'Length of formatted dataframe is not equal to 1, a scraping error might have occurred.'
        return int(df.loc[0]['ConProb'])

    def get_demographic_cases_df(self, x_session_id):
        df = self._df_from_url(self.DEMOGRAPHIC_CASES_DATA_URL.format(x_session_id), index_col=0)
        assert 'Black or African American' in df.index, 'Index name not found.'
        assert 'Count' in df.columns, 'Column name not found'
        return df

    def get_demographic_deaths_df(self, x_session_id):
        df = self._df_from_url(self.DEMOGRAPHIC_DEATHS_DATA_URL.format(x_session_id), index_col=0)
        assert 'Black or African American' in df.index, 'Index name not found.'
        assert 'Deaths' in df.columns, 'Column name not found'
        return df

    def _scrape(self, **kwargs):
        results = self.setup_session()
        assert results.x_session_id, 'No X-Session-Id found'

        demographic_cases_df = self.get_demographic_cases_df(results.x_session_id)
        demographic_deaths_df = self.get_demographic_deaths_df(results.x_session_id)

        date = self.get_date()
        cases = self.get_total_cases(results.x_session_id)
        deaths = int(demographic_deaths_df['Deaths'].sum())
        aa_cases = int(demographic_cases_df.loc['Black or African American', 'Count'])
        aa_deaths = int(demographic_deaths_df.loc['Black or African American', 'Deaths'])
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
