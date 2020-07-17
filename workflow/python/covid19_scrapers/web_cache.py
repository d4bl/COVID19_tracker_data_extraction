# Helpers for cached HTTP data retrieval.
import datetime
import email.utils as eut
import logging
import pickle
import sqlite3
from urllib.parse import urldefrag

import requests


_logger = logging.getLogger(__name__)


def get_current_age(response):
    """Approximate RFC 2616 calculations for cached page age in seconds."""
    now = datetime.datetime.now(datetime.timezone.utc)
    date = response.headers.get('date') or response.headers.get('date')
    if date:
        date_value = eut.parsedate_to_datetime(date)
    else:
        # No date; fall back to a default that will cause revalidation.
        date_value = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    initial_age = max(float(response.headers.get('age', 0)),
                      response.elapsed.total_seconds())
    resident_time = (now - date_value).total_seconds()
    current_age = initial_age + resident_time
    return current_age


def parse_cache_control(response):
    cache_control = {}
    for item in response.headers.get('cache-control', '').split(','):
        idx = item.find('=')
        if idx >= 0:
            cache_control[item[:idx].strip()] = item[idx + 1:].strip()
        else:
            cache_control[item.strip()] = True
    return cache_control


def get_freshness_lifetime(response):
    """Perform RFC 2616 calculations for freshness lifetime."""
    # Parse the cache-control header
    cache_control = parse_cache_control(response)

    # If there is a no-cache cache control, check the server.
    if cache_control.get('no-cache'):
        return 0.0

    # If there is a max-age cache control, use it.
    max_age = cache_control.get('max-age')
    if max_age is not None:
        return float(max_age)

    # Fall back to Expires, if present.
    expires = response.headers.get('expires')
    if expires is not None:
        date_value = eut.parsedate_to_datetime(
            response.headers['date'])
        max_age = (eut.parsedate_to_datetime(expires)
                   - date_value).total_seconds()
        return max_age

    # Fall back to 4 hours, otherwise.
    return 4 * 3600.0


def is_fresh(response):
    """Perform RFC 2616 calculations for page expiration."""
    lifetime = get_freshness_lifetime(response)
    age = get_current_age(response)
    if lifetime > age:
        _logger.debug(f'Response is fresh: max_age={lifetime} > age={age}')
        return True
    else:
        _logger.debug(f'Response is stale: max_age={lifetime} <= age={age}')
        return False


class WebCache(object):
    SCHEMA = [
        'url TEXT PRIMARY KEY',
        'etag TEXT',
        'last_modified TEXT',
        'response BLOB NOT NULL',
    ]

    def __init__(self, db_name='web_cache.db', reset=False):
        # Set up DB connection.
        _logger.info(f'Connecting web cache to DB: {db_name}')
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        if reset:
            _logger.debug('Resetting DB table')
            self.cursor.execute('DROP TABLE IF EXISTS web_cache')
        _logger.debug('Creating DB table')
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS web_cache\n'
            f'({", ".join(self.SCHEMA)})')

    def __repr__(self):
        return f'<{self.__class__.__name__} db={self.db_name}>'

    def delete_from_cache(self, where):
        """Remove the requested rows from the cache.

        Arguments:
          where: a string WHERE clause in the schema of this DB.

        Example:
          web_cache.delete_from_cache(where='url LIKE "%missisippi%"')

        """
        self.cursor.execute('DELETE FROM web_cache '
                            f'WHERE {where}')
        self.conn.commit()

    def get_cached_response(self, url):
        self.cursor.execute(
            'SELECT *'
            ' FROM web_cache WHERE url = ?', (url,))
        row = self.cursor.fetchone()
        if row:
            resp = dict(zip(row.keys(), row))
            if resp['response'] is not None:
                resp['response'] = pickle.loads(resp['response'])
            return resp

    def cache_response(self, url, response, force_cache):
        cache_control = parse_cache_control(response)
        if cache_control.get('no-store'):
            if not force_cache:
                _logger.debug('Skipping cache: response has '
                              'Cache-Control: no-store')
                return
            _logger.debug('Caching: response has Cache-Control: no-store, '
                          'but force_cache is set')

        etag = response.headers.get('etag')
        last_modified = response.headers.get('last-modified')
        if etag is None and last_modified is None:
            if force_cache:
                last_modified = eut.format_datetime(
                    datetime.datetime.now(datetime.timezone.utc))
                _logger.debug('Forcing URL into cache with '
                              f'last-modified={last_modified}')
            else:
                _logger.debug(f'Unable to cache {url}: '
                              'No ETag or Last-Modified header returned')
                return
        self.cursor.execute(
            'INSERT OR REPLACE INTO web_cache VALUES'
            ' (:url, :etag, :last_modified, :response)',
            {
                'url': url,
                'etag': etag,
                'last_modified': last_modified,
                'response': pickle.dumps(response),
            })
        self.conn.commit()

    def touch_response(self, cache_key, cached_response, new_headers):
        """Update the headers in the cached response for cache freshness."""
        cached_response.headers.update(new_headers)
        self.cursor.execute(
            'UPDATE web_cache '
            'SET response=:response '
            'WHERE url=:url',
            {
                'url': cache_key,
                'response': pickle.dumps(cached_response),
            })
        self.conn.commit()

    def fetch(self, url, force_remote=False, force_cache=False,
              cache_only=False, method='GET', headers={}, params={},
              data={}, files={}, cookies={}, session=None,
              session_kwargs={}, **kwargs):
        """Retrieve a URL from the cache, or retrieve the URL from the web and
        store the response into a cache.

        Arguments:
          url: the URL to retrieve.
          force_remote: if True, retrieve the URL without using the cache.
          force_cache: if True, store the URL contents into the cache
            when not present, regardless of missing Last-Modified or
            ETag headers.
          cache_only: if True, only retrieve the URL from the cache.
          method: the HTTP method to use. Only GET requests are cached.
          headers: request headers.
          params: request query parameters.
          data: data for POST requests.
          files: files for POST requestsd.
          cookies: request cookies.
          session: the requests.Session object to use. If one is not
            provided, a new one will be created.
          session_args: dict of arguments to use if constructing a
            requests.Session object.

        Returns a requests.Response object.

        """
        session = session or requests.Session(**session_kwargs)
        request = session.prepare_request(
            requests.Request(method, url=url, headers=headers,
                             cookies=cookies, params=params, data=data,
                             files=files))
        # We want the cache key to include query params, but omit
        # fragment.
        cache_key, _ = urldefrag(request.url)
        cached = None
        revalidating = False

        # HTTP only cache GETs
        if method != 'GET' or force_remote:
            response = session.send(request)
            response.raise_for_status()
            return response

        cached = self.get_cached_response(cache_key)
        if cached:
            _logger.debug(f'Found cache entry: {cache_key}')
            # Are we demanding the cached value?
            if cache_only:
                _logger.debug('Requested cache_only: returning cached'
                              ' response')
                return cached['response']
            # Do we know the cached value is good without revalidating?
            if is_fresh(cached['response']):
                _logger.debug('Cache hit: returning cached response')
                return cached['response']
            # Prepare to revalidate.
            _logger.debug('Revalidating stale cached response')
            if cached['etag']:
                revalidating = True
                request.headers['If-None-Match'] = cached['etag']
            if cached['last_modified']:
                revalidating = True
                request.headers['If-Modified-Since'] = \
                    cached['last_modified']
        else:
            _logger.debug(f'Cache miss: {cache_key}')
            if cache_only:
                raise RuntimeError('Cache miss with cache_only set: '
                                   f'{cache_key}')

        # For cache misses and revalidation, we need to contact the server.
        _logger.debug(f'Sending request: {url}')
        response = session.send(request)

        if revalidating:
            if response.status_code == 304:
                _logger.debug('Still valid: returning cached response')
                # Update the cached headers
                self.touch_response(cache_key, cached['response'],
                                    response.headers)
                return cached['response']
            _logger.debug('No longer valid; replacing cached response')

        response.raise_for_status()
        self.cache_response(cache_key, response, force_cache=force_cache)
        response.headers['x-new-response'] = '1'
        return response
