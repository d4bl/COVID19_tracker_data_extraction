import pydash
from selenium.common.exceptions import InvalidArgumentException


class DriverExpectedConditionsException(Exception):
    pass


class NumberOfElementsIsGreaterOrEqualTo(object):
    def __init__(self, locator, number):
        self.locator = locator
        self.number = number
        assert isinstance(self.number, int), 'number must be an integer'

    def __call__(self, driver):
        try:
            elements = driver.find_elements(*self.locator)
            return len(elements) >= self.number
        except InvalidArgumentException:
            raise DriverExpectedConditionsException('`locator` %s is invalid' % self.locator)


class WaitForResponseFromRequest(object):
    def __init__(self, find_by):
        self.find_by = find_by

    def __call__(self, driver):
        found_request = pydash.find(driver.requests, self.find_by)
        return found_request and found_request.response
