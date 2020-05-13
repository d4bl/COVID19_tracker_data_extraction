from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.states import *

from pathlib import Path


def MakeScraperRegistry(*, home_dir, registry_args={}, scraper_args={}):
    """Makes a default registry with all the states' scrapers.
    """
    home_dir = Path(home_dir)
    registry = Registry(home_dir=home_dir, **registry_args)
    for subclass in ScraperBase.__subclasses__():
        if subclass.__name__.find('Test') < 0:
            registry.register_scraper(
                subclass(home_dir=home_dir / subclass.__name__,
                         **scraper_args))
    return registry
