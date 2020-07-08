# Helpers for cached HTTP data retrieval.
import logging
import pickle
import sqlite3
from urllib.parse import urldefrag

import requests


_logger = logging.getLogger(__name__)


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

    def cache_response(self, url, response):
        etag = response.headers.get('etag')
        last_modified = response.headers.get('last-modified')
        if etag is None and last_modified is None:
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

    def fetch(self, url, force_remote=False, cache_only=False,
              method='GET', headers={}, params={}, data={},
              files={}, cookies={}, session=None, session_kwargs={},
              **kwargs):
        """Retrieve a URL from the cache, or retrieve the URL from the web and
        store the response into a cache.

        Arguments:
          url: the URL to retrieve.
          force_remote: if True, retrieve the URL without using the cache.
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
        # Only use cache for GETs
        if method == 'GET' and not force_remote:
            cached = self.get_cached_response(cache_key)
            if cached:
                _logger.debug(f'cache hit: {cache_key}')
                if cached['etag']:
                    request.headers['If-None-Match'] = cached['etag']
                if cached['last_modified']:
                    request.headers['If-Modified-Since'] = \
                        cached['last_modified']
            else:
                _logger.debug(f'cache miss for {cache_key}')
        if cache_only:
            if cached:
                return cached['response']
            raise RuntimeError(f'Cache miss with cache_only set: {cache_key}')

        _logger.debug(f'Sending request: {url}')
        response = session.send(request)
        if response.status_code == 304:
            # We can only get 304 for conditional GETs, so we know
            # cached is valid.
            _logger.debug(f'Using cached response: {cache_key}')
            return cached['response']
        response.raise_for_status()
        if cached:
            _logger.debug('Cached response is stale')
        self.cache_response(cache_key, response)
        return response
