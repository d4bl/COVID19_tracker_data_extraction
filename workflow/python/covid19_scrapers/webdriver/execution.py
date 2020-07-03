import abc
from copy import deepcopy

from bs4 import BeautifulSoup

from covid19_scrapers import utils


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

    def wait_for(self, conditions, timeout=30):
        return self.add_step(WaitFor(conditions, timeout))

    def find_element_by_xpath(self, xpath, ignore_missing=False):
        return self.add_step(FindElement('xpath', xpath, ignore_missing))

    def click_on_last_element_found(self):
        return self.add_step(ClickOn(last_element=True))

    def get_x_session_id(self):
        return self.add_step(GetXSessionId())

    def get_page_source(self, as_soup=True):
        return self.add_step(GetPageSource(as_soup))


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
        return f"GoToURL(url={self.url})"


class WaitFor(ExecutionStep):
    """Tells the driver to wait for the given conditions before proceeding to the next steps
    """
    def __init__(self, conditions, timeout=30):
        self.conditions = conditions
        self.timeout = timeout
    
    def execute(self, driver, context):
        utils.wait_for_conditions_on_webdriver(driver, self.conditions, self.timeout)

    def __repr__(self):
        return f"WaitFor(conditions={self.conditions}, timeout={self.timeout})"


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
                assert element, "No element found!"
            if self.context_key:
                context.add_to_context(self.context_key, element)
            context.add_to_context('last_element_found', element)
        else:
            raise ExecutionStepException("Method (%s) of finding an element is invalid." % self.method)

    def __repr__(self):
        return (
            f"FindElement(method={self.method}, xpath={self.xpath},"
            f"ignore_missing={self.ignore_missing}, context_key={self.context_key})")
        

class ClickOn(ExecutionStep):
    """After `FindElement` has been invoked in a previous step, click on the last_element_found or
    click on an element saved by key
    """
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
    
    def __repr__(self):
        return f"ClickOn(last_element={self.last_element}, saved_element_name={self.saved_element_name})"


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
        return f"GetPageSource(as_soup={self.as_soup})"


class GetXSessionId(ExecutionStep):
    """Many Tableau dashboards can be interacted with via a X-Session-ID. This function goes through
    the many requests that were made and saves the X-Session-Id. This info can then be obtained via
    the `.get_x_session_id()` function.
    """
    def execute(self, driver, context):
        context.add_to_context(
            'x_session_id', utils.get_session_id_from_seleniumwire(driver))

    def __repr__(self):
        return "GetXSessionId()"
