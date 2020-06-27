from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.states import *

from pathlib import Path


def get_scraper_classes():
    """Generator for non-test subclasses of ScraperBase."""
    for scraper_class in ScraperBase.__subclasses__():
        if scraper_class.__name__.find('Test') < 0:
            yield scraper_class


def get_scraper_names(enable_beta_scrapers=False):
    """Generator for pairs of scraper names and beta status.

    Keyword arguments:
      enable_beta_scrapers: optional, a bool indicating whether to
        include scrapers with the BETA_SCRAPER class variable set in
        run_all_scrapers.

    """
    for scraper_class in get_scraper_classes():
        is_beta = scraper_class.is_beta()
        if is_beta and not enable_beta_scrapers:
            continue
        yield scraper_class.__name__, is_beta


def make_scraper_registry(*, home_dir=Path('work'),
                          registry_args={},
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
