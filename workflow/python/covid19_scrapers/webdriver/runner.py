from collections import namedtuple

from seleniumwire import webdriver


WebdriverResults = namedtuple('WebdriverResults', [
    'x_session_id', 
    'page_source'
])


class WebdriverRunner(object):
    """WebdriverRunner wraps a selenium webdriver and runs steps in a declarative manner allowing for better readabiity
    
    In performing some webscraping tasks, multiple functions are often used together to
    setup various things (generating links, sessions, etc.) such that data can be easily scraped

    To use this, declare multiple steps that need to be invoked. Those actions will be saved in a queue and
    will be lazily run. Invoking the `.run()` method will execute everything in the queue.
    """

    def __init__(self, driver=None):
        self.driver = driver

    def _get_default_driver(self, headless):
        options = webdriver.ChromeOptions()
        if headless is True:
            options.add_argument('headless')
        return webdriver.Chrome(options=options)

    def run(self, webdriver_steps, headless=True):
        """Performs all the steps from webdriver_steps after another in order.

        returns results as a WebdriverResults namedtuple
        """
        driver = self.driver or self._get_default_driver(headless)
        cxt = WebdriverContext()
        for step in webdriver_steps.steps():
            step.execute(driver, cxt)
        if not self.driver:
            driver.quit()
        return cxt.get_results()


class WebdriverContext(object):
    """WebdriverContext is a class that should only be instantiated by WebdriverRunner

    It serves two purposes:
        1. to hold state during a WebdriverRunner().run()
        2. to process results after a run has been complete

    ExecutionSteps should only interact with it only via the `get` and `add_to_context` methods
    """
    def __init__(self):
        self._context = {}

    def __contains__(self, key):
        return key in self._context

    def get(self, key):
        return self._context[key]

    def add_to_context(self, key, value):
        self._context[key] = value

    def get_results(self):
        results = {}
        for field in WebdriverResults._fields:
            results[field] = self._context.get(field, None)
        self._context.clear()
        return WebdriverResults(**results)
