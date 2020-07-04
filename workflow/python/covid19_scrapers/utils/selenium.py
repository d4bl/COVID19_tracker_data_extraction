import logging

from bs4 import BeautifulSoup

import selenium
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException


_logger = logging.getLogger(__name__)


# src: https://stackoverflow.com/questions/45448994/wait-page-to-load-before-getting-data-with-requests-get-in-python-3
def url_to_soup_with_selenium(url, wait_conditions=None, timeout=10):
    """Some site makes multiple requests to load the data.  Calling the
    url alone doesn't return the rendered page.  As a result, selenium
    is being used to allow for the multiple requests to finish.

    """
    # TODO: make this work with get_cached_url?
    options = selenium.webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    driver = selenium.webdriver.Chrome(chrome_options=options)
    driver.get(url)
    if wait_conditions:
        try:
            wait_for_conditions_on_webdriver(driver, wait_conditions, timeout)
        except TimeoutException:
            _logger.error('Timed out waiting for element in URL: %s' % url)
            raise
    return BeautifulSoup(driver.page_source, 'lxml')


def wait_for_conditions_on_webdriver(driver, wait_conditions, timeout=10):
    try:
        conditions = [expected_conditions.presence_of_element_located(wc)
                      for wc in wait_conditions]
        for c in conditions:
            WebDriverWait(driver, timeout).until(c)
    except TimeoutException:
        _logger.error(
            'Waiting for element to load timed out in %s seconds' % timeout)
        raise


def get_session_id_from_seleniumwire(driver):
    responses = [r.response for r in driver.requests if r.response]
    response_headers = [r.headers for r in responses]
    return next((h.get('X-Session-Id')
                 for h in response_headers
                 if 'X-Session-Id' in h), None)
