import re

from bs4 import BeautifulSoup
import pandas as pd
import pytest
import requests

from covid19_scrapers.testing_utils import fake_webcache
import covid19_scrapers.utils as utils


def test_as_list():
    assert utils.as_list('') == ['']
    assert utils.as_list(['']) == ['']


def test_url_to_soup():
    cache, session = fake_webcache()
    with utils.UTILS_WEB_CACHE.with_instance(cache):
        # Good HTML
        session.add_response(content='<html><body>Body</body></html>')
        soup = utils.url_to_soup('http://fake')
        assert isinstance(soup, BeautifulSoup)
        assert soup.find('body') is not None
        assert soup.find('body').text == 'Body'

        # Bad HTML
        session.add_response(content='<html><>Body</body></html>')
        soup = utils.url_to_soup('http://fake')
        assert isinstance(soup, BeautifulSoup)
        assert soup.find('body') is None

        # Exception
        session.add_response(requests.RequestException('error'))
        with pytest.raises(requests.RequestException):
            soup = utils.url_to_soup('http://fake')


def test_raw_string_to_int():
    assert 1 == utils.raw_string_to_int('1')
    assert 1 == utils.raw_string_to_int('1a')
    assert 1000 == utils.raw_string_to_int('1,000')
    assert 100 == utils.raw_string_to_int('100%')
    with pytest.raises(ValueError):
        utils.raw_string_to_int('')
    with pytest.raises(ValueError):
        utils.raw_string_to_int('aaaa')


def test_find_all_links():
    cache, session = fake_webcache()
    content = b"""<html>
  <body>
    <ul>
      <li><a href="link1">
      <li><a href="link2">
      <li><a href="link3">
      <li><a href="link4">
  <body>
</html>"""
    with utils.UTILS_WEB_CACHE.with_instance(cache):
        session.add_response(content=content)
        links = utils.find_all_links('http://fake')
        assert links == ['link1', 'link2', 'link3', 'link4', ]

        session.add_response(content=content)
        links = utils.find_all_links('http://fake', '3')
        assert links == ['link3']

        session.add_response(content=content)
        links = utils.find_all_links('http://fake', re.compile('[34]'))
        assert links == ['link3', 'link4']


def test_table_to_dataframe():
    table = """<table>
  <thead>
    <tr><th>str</th><th>int</th><th>flt</th><th>na</th></tr>
  </thead>
  <tbody>
    <tr></tr>
    <tr><td>str0</td><td>1</td><td>1.0</td></tr>
    <tr><td>str1</td><td>2 </td><td>2.0 </td></tr>
    <tr><td>str2</td><td>1,000</td><td>1,000.0</td><td>NA</td></tr>
    <tr><td>str3</td><td>100%</td><td> 100%</td></tr>
  </tbody>
</table>"""
    df = utils.table_to_dataframe(BeautifulSoup(table, features='lxml'))
    print(df)
    expected = pd.DataFrame(
        columns=['str', 'int', 'flt'],
        data=[['str0', 1, 1.0],
              ['str1', 2, 2.0],
              ['str2', 1000, 1000.0],
              ['str3', 100, 100.0]],
        index=[2, 3, 4, 5])
    assert (df[['str', 'int', 'flt']] == expected).all(axis=None)
    assert pd.isna(df['na']).all()
