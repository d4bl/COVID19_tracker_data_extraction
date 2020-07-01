import csv
import logging
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from seleniumwire import webdriver

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils import (raw_string_to_int, to_percentage,
                                    url_to_soup, url_to_soup_with_selenium,
                                    wait_for_conditions_on_webdriver)

_logger = logging.getLogger(__name__)


def get_demographic_dataframe():
    """This function scrapes data from a public tableau dashboard.

    In order to scrape this data, several steps have to be done:
    1. Issue a request to the `BASE_URL.` Selenium is used here because multiple back and forth requests are
       needed to generate a valid "session"
    2. After a valid session is generated, we scrape the X-Session-Id from the response headers.
       Selenium-wire is used specifically used here because it allows for request/response inspection.
    3. Once the X-Session-Id is scraped, that can be used to query another URL which contains the download link for
       Demographic data. A gotcha here is that the first request will fail, but subsequent requests will succeed.
    4. Once the subsequent request succeeds, we can find the CSV download link and obtain the data.
    """
    BASE_URL = 'https://public.tableau.com/views/NCDHHS_COVID-19_DataDownload/Demographics'
    # Setup selenium wire and make the initial request
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(options=options)
    driver.get(BASE_URL)
    wait_for_conditions_on_webdriver(driver, [(By.XPATH, "//span[contains(text(),'Race')]")])

    # Inspect the response headers for the `X-Session-Id`
    responses = [r.response for r in driver.requests if r.response]
    response_headers = [r.headers for r in responses]
    session_id = next((h.get('X-Session-Id') for h in response_headers if 'X-Session-Id' in h), None)
    assert session_id, "No X-Session-Id found"
    
    # make requests to fetch for the download link
    # first request returns error, the second request will render the data
    DOWNLOAD_URL = (
        "https://public.tableau.com/vizql/w/NCDHHS_COVID-19_DataDownload/v/Demographics/viewData/"
        f"sessions/{session_id}/views/5649504231100340473_15757585069639442359"
        "?maxrows=200&viz=%7B%22worksheet%22%3A%22TABLE_RACE%22%2C%22dashboard%22%3A%22Demographics%22%7D")
    driver.get(DOWNLOAD_URL)
    wait_for_conditions_on_webdriver(driver, [(By.XPATH, "//div[@id='tabBootErrTitle' and contains(text(),'Unexpected Error')]")])
    driver.quit()
    soup = url_to_soup_with_selenium(
        DOWNLOAD_URL,
        wait_conditions=[(By.CLASS_NAME, "csvLink_summary")])
    link = soup.find('a', {'class': 'csvLink_summary'})
    assert link, 'No CSV link found'
    csv_href = link.get('href')
    assert csv_href, 'No CSV link found'

    # Download CSV info and convert/format it into a DataFrame and return
    data = requests.get(csv_href)
    decoded = data.content.decode('utf-8-sig')
    cr = csv.reader(decoded.split('\n'), delimiter=',')
    df = pd.DataFrame(list(cr))
    
    # first row as dataframe header
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header

    # Multiindex Race and Category
    df = df.set_index(['Race', 'Name'])

    # Remaining column is the `value`; rename accordingly
    assert len(df.columns) == 1
    return df.rename(columns={df.columns[0]: 'Value'})


class NorthCarolina(ScraperBase):
    """North Carolina COVID data comes from 2 sources: the main URL and a tableau dashboard

    The main URL consists of the total COVID cases and deaths which can be scraped via `url_to_soup`.
    The dashboard consists of information pertaining to demographics, which is scraped
    via the `get_demographic_dataframe` function in this file.
    """
    URL = 'https://covid19.ncdhhs.gov/'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'North Carolina'

    def get_date(self, soup):
        pattern = re.compile("(\w+ \d{1,2}, \d{4})")
        date_sentence = soup.find(text=pattern)
        assert date_sentence, "No date found"
        results = pattern.search(str(date_sentence))
        assert results, "No date found"
        date_string = results.group()
        return datetime.strptime(date_string, "%B %d, %Y").date()

    def get_total_cases(self, soup):
        text = soup.find(text='Laboratory-Confirmed Cases')
        assert text, "Text not found"
        return raw_string_to_int(text.previous_element)

    def get_total_deaths(self, soup):
        text = soup.find(text='Deaths')
        assert text, "Text not found"
        return raw_string_to_int(text.previous_element)

    def get_aa_cases(self, df):
        return raw_string_to_int(df.loc[('Black or African American', 'Cases'), 'Value'])

    def get_aa_deaths(self, df):
        return raw_string_to_int(df.loc[('Black or African American', 'Deaths'), 'Value'])

    def _scrape(self, **kwargs):
        soup = url_to_soup(self.URL, local_file_name='nc_soup')
        demographic_df = get_demographic_dataframe()

        date = self.get_date(soup)
        cases = self.get_total_cases(soup)
        deaths = self.get_total_deaths(soup)
        aa_cases = self.get_aa_cases(demographic_df)
        aa_deaths = self.get_aa_deaths(demographic_df)
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
