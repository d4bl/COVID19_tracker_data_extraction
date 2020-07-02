from collections import deque

from bs4 import BeautifulSoup
from seleniumwire import webdriver

from covid19_scrapers import utils


class WebdriverRunner(object):
    """WebdriverRunner wraps a selenium webdriver and runs steps in a declarative manner allowing for better readabiity
    
    In performing some webscraping tasks, multiple functions are often used together to
    setup various things (generating links, sessions, etc.) such that data can be easily scraped

    To use this, declare multiple steps that need to be invoked. Those actions will be saved in a queue and
    will be lazily run. Invoking the `.run()` method will execute everything in the queue.
    """
    def __init__(self):
        self._driver = None
        self._steps = deque()
        self.last_element_found = None
        self.cached_x_session_id = None

    def go_to_url(self, url):
        """Tells the driver to go to the url given
        """
        self._steps.append(
            lambda driver: driver.get(url))
        return self

    def wait_for(self, conditions, timeout=30):
        """Tells the driver to wait for the given conditions before proceeding to the next steps
        """
        self._steps.append(
            lambda driver: utils.wait_for_conditions_on_webdriver(driver, conditions, timeout))
        return self

    def find_element_by_xpath(self, xpath, ignore_missing=False):
        """Finds an element by x-path. Element is then saved as a variable which can then be
        used in subsequent actions.
        """
        def _find_element(driver):
            element = driver.find_element_by_xpath(xpath)
            if not ignore_missing:
                assert element, "No element found!"
            self.last_element_found = element
        self._steps.append(_find_element)
        return self

    def click_on_last_element_found(self):
        """After `find_element_by_{}` has been invoked, this function will perform a click on the last
        element that was found.
        """
        self._steps.append(
            lambda driver: self.last_element_found.click())
        return self

    def cache_x_session_id(self):
        """Many Tableau dashboards can be interacted with via a X-Session-ID. This function goes through
        the many requests that were made and saves the X-Session-Id. This info can then be obtained via
        the `.get_x_session_id()` function.
        """
        def _cache(driver):
            self.cached_x_session_id = utils.get_session_id_from_seleniumwire(driver)
        self._steps.append(_cache)
        return self

    def _init_driver(self, headless):
        options = webdriver.ChromeOptions()
        if headless is True:
            options.add_argument('headless')
        self._driver = webdriver.Chrome(options=options)
    
    def run(self, headless=True):
        """Performs all the actions queued up after another in order.

        Each action is appended to a queue as a function so during execution
        each function will be popped off the queue and invoked.
        """
        self._init_driver(headless)
        while self._steps:
            step_fn = self._steps.popleft()
            step_fn(self._driver)

    def get_x_session_id(self):
        """If there exists a cached X-Session-Id, this function will return that.
        Otherwise, it will go through all the responses recieved and find a X-Session-Id, cache it then return it.
        """
        assert self._driver, "No driver initialized, execute the `run()` method first."
        if not self.cached_x_session_id:
            self.cached_x_session_id = utils.get_session_id_from_seleniumwire(self._driver)
        return self.cached_x_session_id

    def get_page_source_as_soup(self):
        return BeautifulSoup(self._driver.page_source, 'lxml')

    def quit(self):
        self._driver.quit()
