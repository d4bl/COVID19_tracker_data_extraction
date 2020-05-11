from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.states import *

def MakeScraperRegistry(registry_args={}, scraper_args={}):
    """Makes a default registry with all the states' scrapers.
    """
    registry = Registry(**registry_args)
    for subclass in ScraperBase.__subclasses__():
        if subclass.__name__.find('Test') < 0:
            registry.register_scraper(subclass(**scraper_args))
    return registry
