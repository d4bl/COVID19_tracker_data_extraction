import email.utils as eut
from io import BytesIO
from pathlib import Path
import os
import logging
from urllib.parse import urlsplit

import requests
import ssl

from covid19_scrapers.utils import UTILS_WEB_CACHE


ssl._create_default_https_context = ssl._create_unverified_context
_logger = logging.getLogger(__name__)


# Helpers for HTTP data retrieval.
def get_http_datetime(url):
    """Find the Last-Modified time for the URL from the cache, if present,
    or from a HEAD response.

    """
    try:
        r = get_cached_url(url, cache_only=True)
    except RuntimeError:
        _logger.debug(f'trying HEAD request for {url}')
        r = requests.head(url)
        r.raise_for_status()
    date = r.headers.get('last-modified')
    if date:
        return eut.parsedate_to_datetime(date)


def get_http_date(url):
    """Find the date from the Last-Modified time for the URL from
    the cache, if present, or from a HEAD response.

    """
    date = get_http_datetime(url)
    if date:
        return date.date()


def get_cached_url(url, **kwargs):
    """Retrieve a URL from the cache, or retrieve the URL from the web and
    store the response into a cache.

    This must be called inside
      with UTILS_WEB_CACHE(...):
         ...

    Arguments:
      url: the URL to retrieve

    Returns a requests.Response object.

    """
    return UTILS_WEB_CACHE.fetch(url, **kwargs)


def download_file(file_url, new_file_name=None, **kwargs):
    """Save the url contents in the specified file."""
    if new_file_name is None:
        new_file_name = Path(urlsplit(file_url).path).name
    try:
        response = get_cached_url(file_url, **kwargs)
        new_file = Path(new_file_name)
        _logger.debug(f'Saving response content to: {new_file}')
        if new_file.parent and not new_file.parent.exists():
            _logger.debug(f'Making {new_file.parent}')
            os.makedirs(str(new_file.parent), exist_ok=True)
        with new_file.open('wb') as f:
            f.write(response.content)
    except Exception as e:
        _logger.warn(f'File download failed: {new_file_name}: {e}')
        raise


def get_json(url, **kwargs):
    """Return the url's reponse contents as parsed JSON.

    This can raise a requests.RequestException if retrieval fails, or
    a ValueError if the JSON cannot be decoded.
    """
    # The next line can raise a requests.RequestException
    r = get_cached_url(url, **kwargs)
    # The next line can raise a ValueError
    return r.json()


def get_content(url, **kwargs):
    """Return the url's reponse contents as bytes."""
    r = get_cached_url(url, **kwargs)
    return r.content


def get_content_as_file(url, **kwargs):
    """Return the url's reponse contents as a BytesIO."""
    return BytesIO(get_content(url, **kwargs))
