import os
import sqlite3

import pytest
import requests

from covid19_scrapers.web_cache import WebCache
from covid19_scrapers.testing_utils import MockSession, fake_webcache


def test_webcache_delete_uncached_fails():
    webcache = WebCache(':memory:')
    with pytest.raises(sqlite3.DatabaseError):
        webcache.delete_from_cache('url = http://fake/')


def test_webcache_delete_incache_succeeds():
    webcache = WebCache(':memory:')
    r = requests.Response()
    r.status_code = 200
    r._content = b'Content'
    r.encoding = 'utf-8'
    r.headers['ETag'] = 'fake-etag'
    r = webcache.cache_response('http://fake/', r)
    webcache.delete_from_cache('url = "http://fake/"')


def test_webcache_get_uncached_fails():
    webcache = WebCache(':memory:')
    row = webcache.get_cached_response('http://fake/')
    assert row is None


def test_webcache_get_incache_succeeds():
    session = MockSession()
    r = session.make_response(content=b'Content')
    r.headers['ETag'] = 'fake-etag'

    webcache = WebCache(':memory:')
    webcache.cache_response('http://fake/', r)
    r2 = webcache.get_cached_response('http://fake/')
    assert isinstance(r2, dict)
    assert len(r2) == 4
    assert r2['url'] == 'http://fake/'
    assert r2['etag'] == r2['response'].headers['ETag']
    assert r.status_code == r2['response'].status_code
    assert r.content == r2['response'].content
    assert r.encoding == r2['response'].encoding
    assert r.headers['ETag'] == r2['response'].headers['ETag']


def test_reset():
    session = MockSession()
    r = session.make_response(content=b'Content')
    r.headers['ETag'] = 'fake-etag'

    try:
        webcache = WebCache('test.db', reset=True)
        webcache.cache_response('http://fake/', r)
        r2 = webcache.get_cached_response('http://fake/')
        webcache.conn.close()
        assert r2 is not None

        webcache = WebCache('test.db', reset=True)
        r3 = webcache.get_cached_response('http://fake/')
        webcache.conn.close()
        assert r3 is None
    finally:
        os.remove('test.db')


def test_webcache_fetch_nocache():
    webcache, session = fake_webcache()
    r = session.make_response()
    r.headers['etag'] = 'fake-etag'
    session.add_response(r)
    webcache.cache_response('http://fake/', r)
    resp = webcache.fetch('http://fake/')
    print(resp, r)
    # Check that we got back the same Response instance
    assert resp == r


def test_webcache_fetch_incache():
    webcache, session = fake_webcache()
    r = session.make_response(status_code=200)
    r.headers['ETag'] = 'fake-etag'
    webcache.cache_response('http://fake/', r)
    r.status_code = 304
    session.add_response(r)

    resp = webcache.fetch('http://fake/')
    assert resp.headers['etag'] == r.headers['etag']
    assert resp != r, 'Expected new Response instance'


def test_webcache_fetch_stale():
    webcache, session = fake_webcache()
    r_stale = session.make_response(status_code=200)
    r_stale.headers['ETag'] = 'fake-etag'
    webcache.cache_response('http://fake/', r_stale)
    session.add_response(r_stale)

    r_new = session.make_response(status_code=200)
    r_new.headers['ETag'] = 'new-etag'
    session.add_response(r_new)

    resp = webcache.fetch('http://fake/')
    assert resp.headers['etag'] == r_new.headers['etag']
    assert resp == r_new


def test_webcache_fetch_incache_force_remote():
    webcache, session = fake_webcache()
    r = session.make_response(status_code=200)
    r.headers['ETag'] = 'fake-etag'
    webcache.cache_response('http://fake/', r)
    r.headers['ETag'] = 'new-etag'
    session.add_response(r)

    resp = webcache.fetch('http://fake/', force_remote=True)
    assert resp.headers['ETag'] == 'new-etag'
    assert resp == r


def test_webcache_fetch_uncached_force_remote():
    webcache, session = fake_webcache()
    r = session.make_response(status_code=200)
    r.headers['ETag'] = 'fake-etag'
    session.add_response(r)

    resp = webcache.fetch('http://fake/', force_remote=True)
    assert resp == r


def test_webcache_fetch_incache_cache_only():
    webcache, session = fake_webcache()
    r = session.make_response(status_code=200)
    r.headers['ETag'] = 'fake-etag'
    webcache.cache_response('http://fake/', r)
    session.add_response(r)

    resp = webcache.fetch('http://fake/', cache_only=True)
    assert resp.headers['ETag'] == r.headers['ETag']
    assert resp != r


def test_webcache_fetch_uncached_cache_only():
    webcache, session = fake_webcache()
    r = session.make_response(status_code=200)
    r.headers['ETag'] = 'fake-etag'
    session.add_response(r)
    with pytest.raises(RuntimeError):
        webcache.fetch('http://fake/', cache_only=True)
