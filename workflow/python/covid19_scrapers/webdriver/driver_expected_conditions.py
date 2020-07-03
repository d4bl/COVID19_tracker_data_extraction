class NumberOfElementsIsGreaterOrEqualTo(object):
    def __init__(self, locator, number):
        self.locator = locator
        self.number = number

    def __call__(self, driver):
        return len(driver.find_elements(*self.locator)) >= self.number
