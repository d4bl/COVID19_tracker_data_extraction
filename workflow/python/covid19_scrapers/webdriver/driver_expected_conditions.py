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
