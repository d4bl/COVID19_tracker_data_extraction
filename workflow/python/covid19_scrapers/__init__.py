from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.states import *

from pathlib import Path


def get_scraper_classes():
    for scraper_class in ScraperBase.__subclasses__():
        if scraper_class.__name__.find('Test') < 0:
            yield scraper_class


def get_scraper_names():
    for scraper_class in get_scraper_classes():
        yield scraper_class.__name__


def make_scraper_registry(*, home_dir=Path('work'), registry_args={},
                          scraper_args={}):
    """Returns a Registry instance with all the per-state scrapers
    registered.

    Keyword arguments:

      home_dir: required, a Pathlike for the root of a working
        directory.  Cached downloads will be saved in per-scraper
        directories under this.  If it does not exist, it will be
        created.

      registry_args: optional, a dict of additional keyword arguments
        for the Registry constructor.

      scraper_args: optional, a dict of additional keyword arguments
        for all scrapers' constructors.

    """
    registry = Registry(**registry_args)
    for scraper_class in get_scraper_classes():
        registry.register_scraper(
            scraper_class(home_dir=home_dir / scraper_class.__name__,
                          **scraper_args))
    return registry
