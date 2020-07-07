import abc
import enum
from copy import deepcopy
import logging

import pydash
from bs4 import BeautifulSoup
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from covid19_scrapers.webdriver.driver_expected_conditions import NumberOfElementsIsGreaterOrEqualTo


_logger = logging.getLogger(__name__)


class WebdriverSteps(object):
    """This class is used to hold the steps that should be executed in order
    by the WebdriverRunner.

    Use the `add_step` function to add an `ExecutionStep` to the list

    Optionally, once an execution step has been created, an instance method, for adding
    the execution step, can be added to this class as (kinda) syntactic sugar.
    """

    def __init__(self):
        self._steps = []

    def add_step(self, step):
        clone = deepcopy(self)
        clone._steps.append(step)
        return clone

    def steps(self):
        return self._steps

    def go_to_url(self, url):
        return self.add_step(GoToURL(url))

    def wait_for_presence_of_elements(self, element_locators, timeout=60):
        return self.add_step(WaitFor(element_locators, timeout=timeout))

    def wait_for_number_of_elements(self, element_locators, number_of_elements, timeout=60):
        return self.add_step(
            WaitFor(element_locators, condition=Condition.NUMBER_OF_ELEMENTS,
                    number_of_elements=number_of_elements, timeout=timeout))

    def find_element_by_xpath(self, xpath, ignore_missing=False):
        return self.add_step(FindElement('xpath', xpath, ignore_missing))

    def click_on_last_element_found(self):
        return self.add_step(ClickOn(last_element=True))

    def get_x_session_id(self):
        return self.add_step(GetXSessionId())

    def get_page_source(self, as_soup=True):
        return self.add_step(GetPageSource(as_soup))

    def find_request(self, key, find_by):
        return self.add_step(FindRequest(key, find_by))


class ExecutionStepException(Exception):
    pass


class ExecutionStep(metaclass=abc.ABCMeta):
    """This class is an abstract class used for declaring a single step for
    the WebdriverRunner to execute.

    Usage:
        class ExampleStep(ExecutionStep):
            def __init__(self, thing):
                self.thing = thing

            def execute(self, driver, context):
                result = driver.do_something_with(self.thing)
                context.add_to_context('thing', result)

            def __repr__(self):
                return f"ExampleStep(thing={self.thing})"
    """

    def __init__(self):
        pass

    @abc.abstractmethod
    def execute(self, driver, context):
        """The function that will be executed by the WebdriverRunner

        Positional arguments:
          driver: the webdriver instance that will be passed in

          context: a `WebdriverContext` used to hold state.
            See WebdriverContext in `covid19_scrapers/webdriver/runner.py`
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __repr__(self):
        """The __repr__ method must be implemented for logging purposes
        """
        raise NotImplementedError


class GoToURL(ExecutionStep):
    """Tells the driver to go to the url given
    """

    def __init__(self, url):
        self.url = url

    def execute(self, driver, context):
        driver.get(self.url)

    def __repr__(self):
        return f'GoToURL(url={self.url})'


class Condition(enum.Enum):
    PRESENCE = 'presence'
    NUMBER_OF_ELEMENTS = 'number_of_elements'


class WaitFor(ExecutionStep):
    """Tells the driver to wait for the given conditions before proceeding to the next steps
    """

    def wait_for_conditions_on_webdriver(self, driver, conditions, timeout):
        try:
            for c in conditions:
                WebDriverWait(driver, timeout).until(c)
        except TimeoutException:
            _logger.error('Waiting timed out in %s seconds' % timeout)
            raise

    def __init__(self, element_locators, condition=Condition.PRESENCE, number_of_elements=None, timeout=60):
        if condition not in Condition:
            raise ExecutionStepException('Invalid condition, check the `Conditions` enum for valid conditions')
        self.locators = self._listify(element_locators)
        self.condition = condition
        self.timeout = timeout
        self.number_of_elements = number_of_elements

    def execute(self, driver, context):
        applied_conditions = None
        if self.condition == Condition.PRESENCE:
            applied_conditions = [expected_conditions.presence_of_element_located(locator)
                                  for locator in self.locators]
        elif self.condition == Condition.NUMBER_OF_ELEMENTS:
            assert isinstance(self.number_of_elements, int), '`number_of_elements` as an integer must be given'
            applied_conditions = [NumberOfElementsIsGreaterOrEqualTo(locator, self.number_of_elements)
                                  for locator in self.locators]
        else:
            raise ExecutionStepException('Invalid condition, check the `Conditions` enum for valid conditions')
        self.wait_for_conditions_on_webdriver(driver, applied_conditions, self.timeout)

    def _listify(self, obj):
        if not isinstance(obj, list):
            return [obj]
        return obj

    def __repr__(self):
        return (
            f'WaitFor(locators={self.locators}, condition={self.condition},'
            f'number_of_elements={self.number_of_elements}, timeout={self.timeout})')


class FindElement(ExecutionStep):
    """Finds an element by parameters given. Element is then saved as a variable which can then be
    used in subsequent ExecutionSteps.
    """

    def __init__(self, method, xpath=None, ignore_missing=False, context_key=None):
        self.method = method
        self.xpath = xpath
        self.context_key = context_key
        self.ignore_missing = ignore_missing

    def execute(self, driver, context):
        if self.method == 'xpath':
            element = driver.find_element_by_xpath(self.xpath)
            if not self.ignore_missing:
                assert element, 'No element found!'
            if self.context_key:
                context.add_to_context(self.context_key, element)
            context.add_to_context('last_element_found', element)
        else:
            raise ExecutionStepException('Method (%s) of finding an element is invalid.' % self.method)

    def __repr__(self):
        return (
            f'FindElement(method={self.method}, xpath={self.xpath},'
            f'ignore_missing={self.ignore_missing}, context_key={self.context_key})')


class ClickOn(ExecutionStep):
    """After `FindElement` has been invoked in a previous step, click on the last_element_found or
    click on an element saved by key
    """

    def __init__(self, last_element=False, saved_element_name=None):
        if not (bool(last_element) ^ bool(saved_element_name)):
            raise ExecutionStepException(
                'Either `last_element` as True or saved_element_name must be given, but not both')
        self.last_element = last_element
        self.saved_element_name = saved_element_name

    def execute(self, driver, context):
        if self.last_element:
            assert 'last_element_found' in context, 'context missing `last_element_found`, cannot click.'
            context.get('last_element_found').click()
        elif self.saved_element_name:
            assert self.saved_element_name in context, 'context missing %s, cannot click.' % self.saved_element_name
            context.get(self.saved_element_name).click()
        else:
            raise ExecutionStepException('Invalid click location')

    def __repr__(self):
        return f'ClickOn(last_element={self.last_element}, saved_element_name={self.saved_element_name})'


class GetPageSource(ExecutionStep):
    """Adds the page_source as raw text or with the option of outputting it as a BeautifulSoup output
    """

    def __init__(self, as_soup):
        self.as_soup = as_soup

    def execute(self, driver, context):
        data = driver.page_source
        if self.as_soup:
            data = BeautifulSoup(data, 'lxml')
        context.add_to_context('page_source', data)

    def __repr__(self):
        return f'GetPageSource(as_soup={self.as_soup})'


class GetXSessionId(ExecutionStep):
    """Many Tableau dashboards can be interacted with via a X-Session-ID. This function goes through
    the many requests that were made and saves the X-Session-Id. This info can then be obtained via
    the `.get_x_session_id()` function.
    """

    def get_session_id_from_seleniumwire(self, driver):
        responses = [r.response for r in driver.requests if r.response]
        response_headers = [r.headers for r in responses]
        return next((h.get('X-Session-Id') for h in response_headers if 'X-Session-Id' in h), None)

    def execute(self, driver, context):
        context.add_to_context(
            'x_session_id', self.get_session_id_from_seleniumwire(driver))

    def __repr__(self):
        return 'GetXSessionId()'


class FindRequest(ExecutionStep):
    """Adds a request to the context under the `requests` variable in WebdriverResults
    `requests` will be a dictionary keyed by the `key` variable

    Params:
        key: the key which the request will be saved to in the `request` variable.
        find_by: function that takes a single seleniumwire.webdriver.request.Request object.
            Requests made by the dricver will then be iterated over and the first request that
            the function returns truthy for will be saved.
    """

    def __init__(self, key, find_by):
        self.key = key
        self.find_by = find_by

    def execute(self, driver, context):
        current = context.get('requests')
        found_request = {self.key: pydash.find(driver.requests, self.find_by)}
        context.add_to_context('requests', {**current, **found_request})

    def __repr__(self):
        return f'GetRequest(key={self.key}, find_by={self.find_by.__name__})'
