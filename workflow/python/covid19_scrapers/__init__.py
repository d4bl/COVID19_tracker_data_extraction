import os
from pathlib import Path

from covid19_scrapers.census import CensusApi
from covid19_scrapers.registry import Registry
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.states import *  # noqa: F401,F403
from covid19_scrapers.utils import UTILS_WEB_CACHE
from covid19_scrapers.web_cache import WebCache


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
                          census_api_key=None,
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

    os.makedirs(str(home_dir), exist_ok=True)
    # We need a web cache for creating the census API.
    web_cache = WebCache(str(home_dir / 'web_cache.db'))
    with UTILS_WEB_CACHE.with_instance(web_cache):
        census_api = CensusApi(census_api_key)
    registry = Registry(web_cache=web_cache, **registry_args)
    for scraper_class in get_scraper_classes():
        registry.register_scraper(
            scraper_class(home_dir=home_dir / scraper_class.__name__,
                          census_api=census_api,
                          **scraper_args))
    return registry
