__all__ = ['UTILS_WEB_CACHE']

from covid19_scrapers.web_cache import WebCache
from covid19_scrapers.scoped_resource import ScopedResource

# Singleton WebCache for the routines in this file.
# Set it using something like:
#
#    with utils.UTILS_WEB_CACHE('my_cache.db'):
#        code_that_might_call_utils()
#
# It is initialized with an in-memory cache, which is discarded the
# first time it is set using `with`.
UTILS_WEB_CACHE = ScopedResource(WebCache).with_instance(WebCache(':memory:'))
