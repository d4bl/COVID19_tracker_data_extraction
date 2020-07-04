import requests

from covid19_scrapers.web_cache import WebCache


class FakeCensusApi(object):
    def get_aa_pop_stats(self):
        return (None, None, None)


class MockSession(object):
    def __init__(self):
        self.responses = []

    def make_response(self, method='GET', status_code=200, headers={},
                      content=b'', encoding='utf-8'):
        r = requests.Response()
        r.status_code = status_code
        r.headers.update(headers)
        if isinstance(content, str):
            content = content.encode(encoding)
        r._content = content
        return r

    def add_response(self, resp=None, **kwargs):
        if resp:
            self.responses.append(resp)
        else:
            self.responses.append(self.make_response(**kwargs))

    def prepare_request(self, request):
        return request.prepare()

    def send(self, request):
        resp = self.responses.pop()
        if isinstance(resp, Exception):
            raise resp
        resp.request = request
        return resp


def fake_webcache():
    wc = WebCache(':memory:')
    old_fetch = wc.fetch
    mock_session = MockSession()

    def new_fetch(url, force_remote=False, cache_only=False,
                  method='GET', headers={}, params={}, data={},
                  files={}, cookies={}, session=None, session_kwargs={},
                  **kwargs):
        return old_fetch(url, force_remote, cache_only, method, headers,
                         params, data, files, cookies, mock_session, {},
                         **kwargs)
    wc.fetch = new_fetch
    return wc, mock_session


def make_cached_response(session, cache, url, method='GET',
                         status_code=200, headers={}, content=b'',
                         encoding='utf-8'):
    resp = session.make_response(method, status_code, headers,
                                 content, encoding)
    resp.headers['etag'] = 'fake-etag'
    cache.cache_response(url, resp)
    session.add_response(session.make_response(status_code=304))
