import abc
from copy import deepcopy

from bs4 import BeautifulSoup

from covid19_scrapers import utils


class WebdriverSteps(object):
    def __init__(self):
        self._steps = []

    def add_step(self, step):
        clone = deepcopy(self)
        clone._steps.append(step)
        return clone

    def steps(self):
        return self._steps
    
    def go_to_url(self, url):
        """Tells the driver to go to the url given
        """
        return self.add_step(GoToURL(url))

    def wait_for(self, conditions, timeout=30):
        """Tells the driver to wait for the given conditions before proceeding to the next steps
        """
        return self.add_step(WaitFor(conditions, timeout))

    def find_element_by_xpath(self, xpath, ignore_missing=False):
        """Finds an element by x-path. Element is then saved as a variable which can then be
        used in subsequent actions.
        """
        return self.add_step(FindElement('xpath', xpath, ignore_missing))

    def click_on_last_element_found(self):
        """After `find_element_by_{}` has been invoked, this function will perform a click on the last
        element that was found.
        """
        return self.add_step(ClickOn(last_element=True))

    def get_x_session_id(self):
        """Many Tableau dashboards can be interacted with via a X-Session-ID. This function goes through
        the many requests that were made and saves the X-Session-Id. This info can then be obtained via
        the `.get_x_session_id()` function.
        """
        return self.add_step(GetXSessionId())

    def get_page_source(self, as_soup=True):
        """Adds the page_source as raw text or with the option of outputting it as a BeautifulSoup output
        """
        return self.add_step(GetPageSource(as_soup))


class ExecutionStepException(Exception):
    pass


class ExecutionStep(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    def execute(self, driver, context):
        raise NotImplementedError


class GoToURL(ExecutionStep):
    def __init__(self, url):
        self.url = url
    
    def execute(self, driver, context):
         driver.get(self.url)


class WaitFor(ExecutionStep):
    def __init__(self, conditions, timeout=30):
        self.conditions = conditions
        self.timeout = timeout
    
    def execute(self, driver, context):
        utils.wait_for_conditions_on_webdriver(driver, self.conditions, self.timeout)


class FindElement(ExecutionStep):
    def __init__(self, method, xpath=None, ignore_missing=False, context_key=None):
        self.method = method
        self.xpath = xpath
        self.context_key = context_key
        self.ignore_missing = ignore_missing
    
    def execute(self, driver, context):
        if self.method == 'xpath':
            element = driver.find_element_by_xpath(self.xpath)
            if not self.ignore_missing:
                assert element, "No element found!"
            if self.context_key:
                context.add_to_context(self.context_key, element)
            context.add_to_context('last_element_found', element)
        else:
            raise ExecutionStepException("Method (%s) of finding an element is invalid." % self.method)


class ClickOn(ExecutionStep):
    def __init__(self, last_element=False, saved_element_name=None):
        if not (bool(last_element) ^ bool(saved_element_name)):
            raise ExecutionStepException(
                "Either `last_element` as True or saved_element_name must be given, but not both")
        self.last_element = last_element
        self.saved_element_name = saved_element_name

    def execute(self, driver, context):
        if self.last_element:
            assert 'last_element_found' in context, "context missing `last_element_found`, cannot click."
            context.get('last_element_found').click()
        elif self.saved_element_name:
            assert self.saved_element_name in context, "context missing %s, cannot click." % self.saved_element_name
            context.get(self.saved_element_name).click()
        else:
            raise ExecutionStepException("Invalid click location")


class GetPageSource(ExecutionStep):
    def __init__(self, as_soup):
        self.as_soup = as_soup
    
    def execute(self, driver, context):
        data = driver.page_source
        if self.as_soup:
            data = BeautifulSoup(data, 'lxml')
        context.add_to_context('page_source', data)


class GetXSessionId(ExecutionStep):
    def execute(self, driver, context):
        context.add_to_context(
            'x_session_id', utils.get_session_id_from_seleniumwire(driver))
