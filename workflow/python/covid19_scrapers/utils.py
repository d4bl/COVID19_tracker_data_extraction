# Misc utilities
from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS
import datetime
import logging
import pandas as pd
from pathlib import Path
import os
import re
from urllib.parse import urlsplit

# Read webpage
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests
from zipfile import ZipFile

# Import selenium dependencies
import selenium
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

# Import packages needed to run GA code
import email.utils as eut
from io import BytesIO
import zipfile
import ssl

from covid19_scrapers.scoped_resource import ScopedResource
from covid19_scrapers.web_cache import WebCache


ssl._create_default_https_context = ssl._create_unverified_context
_logger = logging.getLogger(__name__)
UTILS_WEB_CACHE = ScopedResource(WebCache)


def as_list(arg):
    """If arg is a list, return it.  Otherwise, return a list containing
    it.

    This is to make it easier to consume tabula.read_pdf output, which
    is either a table or a list of tables.
    """
    if isinstance(arg, list):
        return arg
    return [arg]


# Content retrieval and parsing helpers.

def url_to_soup(data_url, **kwargs):
    """
    Converts string into beautiful soup object for parsing

    Parameters
    ----------
    data_url: string
        website link

    Returns
    -------
    data_soup: Beautifulsoup object
        HMTL code from webpage
    """
    try:
        data_page = get_cached_url(data_url, **kwargs)
    except requests.RequestException:
        _logger.warn(f'request failed: {data_url}')
        raise
    _logger.debug(f'request successful: {data_url}')

    # Create a Beautiful Soup object
    data_text = data_page.text
    data_soup = BeautifulSoup(data_text, 'html.parser')

    return data_soup


def raw_string_to_int(s):
    """Some parsed strings have additional elements attached to them such
    as `\n` or `,`.  This function filters those elements out and
    casts the string to an int.

    It throws ValueError if the string is empty.

    """
    return int(''.join([c for c in s if c.isnumeric()]))


def to_percentage(numerator, denominator, round_num_digits=2):
    """Copies of this code are used in almost all the scrapers to
    calculate Black/AA death and case percentages.

    """
    return round((numerator / denominator) * 100, round_num_digits)


# Source: https://stackoverflow.com/questions/1080411/retrieve-links-from-web-page-using-python-and-beautifulsoup

def find_all_links(url, search_string=None):
    resp = requests.get(url)
    http_encoding = resp.encoding if 'charset' in resp.headers.get(
        'content-type', '').lower() else None
    html_encoding = EncodingDetector.find_declared_encoding(resp.content,
                                                            is_html=True)
    encoding = html_encoding or http_encoding
    soup = BeautifulSoup(resp.content, from_encoding=encoding,
                         features='lxml')

    link_list = []

    for link in soup.find_all('a', href=True):
        link_list.append(link['href'])

    if search_string:
        if isinstance(search_string, str):
            return [x for x in link_list if search_string in x]
        elif isinstance(search_string, re.Pattern):
            return [x for x in link_list if search_string.search(x)]
    else:
        return link_list


def _maybe_convert(val):
    val = val.replace(',', '').replace('%', '').replace('NA', 'nan').strip()
    try:
        return float(val)
    except ValueError:
        return val


def table_to_dataframe(table):
    """Given a bs4 Table element, make a DataFrame using the `th` items as
    columns and `td` items as float data.
    """
    columns = [th.text.strip() for th in table.find_all('th')]
    _logger.debug(f'Creating DataFrame with columns {columns}')
    data = [[_maybe_convert(td.text) for td in tr.find_all('td')]
            for tr in table.find_all('tr')]
    return pd.DataFrame(data, columns=columns)


def get_http_datetime(url):
    r = requests.head(url)
    r.raise_for_status()
    date = r.headers.get('last-modified')
    if date:
        return eut.parsedate_to_datetime(date)


def get_http_date(url):
    date = get_http_datetime(url)
    if date:
        return date.date()


# Helpers for ESRI/ArcGIS web services (geoservices)
#
# The big idea is that the query parameters can be declared as a
# single class variable, and applied to the query_geoservice call.
#
# See, eg, states/missouri.py for examples.

def make_geoservice_stat(agg, in_field, out_name):
    """This makes a single entry for the `stats` field of a
    query_geoservice request (a.k.a. the `outStatistics` field of a
    geoservice request).

    """
    return {
        'statisticType': agg,
        'onStatisticField': in_field,
        'outStatisticFieldName': out_name,
    }


def _get_layer_by_name(layer_name, layers, tables):
    for layer in layers:
        if layer.properties.name == layer_name:
            return layer
    for table in tables:
        if table.properties.name == layer_name:
            return table


def _get_layer_by_id(layer_id, layers, tables):
    for layer in layers:
        if layer.properties.id == layer_id:
            return layer
    for table in tables:
        if table.properties.id == layer_id:
            return table


def _get_layer(flc_id, flc_url, layer_name):
    # Get the feature layer collection.
    if flc_id:
        gis = GIS()
        flc = gis.content.get(flc_id)
        loc = f'content ID {flc_id}'
        assert flc is not None, f'Unable to find ArcGIS ID {flc_id}'
    elif flc_url:
        loc = f'flc URL {flc_url}'
        flc = FeatureLayerCollection(flc_url)
    else:
        raise ValueError('Either flc_id or url must be provided')

    # Now get the layer.
    if isinstance(layer_name, str):
        layer = _get_layer_by_name(layer_name, flc.layers, flc.tables)
    elif isinstance(layer_name, int):
        layer = _get_layer_by_id(layer_name, flc.layers, flc.tables)
    if layer:
        return layer
    raise ValueError(f'Unable to find layer {layer_name} in {loc}')


def query_geoservice(*, flc_id=None, flc_url=None, layer_name=None,
                     where='1=1', out_fields=['*'], group_by=None,
                     stats=None, order_by=None, limit=None):
    """Queries the specified ESRI GeoService.

    Mandatory arguments:
      Either of
        flc_id: FeatureLayerCollection ID to search for.
      or
        flc_url: URL for a FeatureServer or MapServer REST endpoint.
      and
        layer_name: the name of the desired layer or table.
      must be provided.

    Optional arguments:
      where: the feature filtering query.
      out_fields: the fields to retrieve, defaults to all.
      group_by: the field by which to group for statistical operations.
      stats: a list of dicts specifying the desired statistical operations.
      order_by: field and direction to order by.
      limit: max number of records to retrieve.

    Returns: a pair consisting of the update date and data frame
      containing the features.
    """
    layer = _get_layer(flc_id, flc_url, layer_name)
    features = layer.query(
        spatialRel='esriSpatialRelIntersects',
        where=where,
        outFields=','.join(out_fields),
        return_geometry=False,
        groupByFieldsForStatistics=group_by,
        outStatistics=stats,
        orderByFields=order_by,
        resultRecordCount=limit,
        resultType='standard')
    try:
        update_date = datetime.datetime.fromtimestamp(
            layer.properties.editingInfo.lastEditDate / 1000).date()
    except AttributeError:
        update_date = None
    return update_date, features.sdf


# Helpers for HTTP data retrieval.
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


# Wrappers to handle zip files
def unzip(path_to_zip_file, directory_to_extract_to='.'):
    """Unzip a zip file by path to a directory, by default the working
    directory.
    """
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)


def get_zip(url):
    """Fetch a zip file by URL and return a ZipFile object to access its
    contents and metadata.
    """
    return ZipFile(BytesIO(get_content(url)))


def get_zip_member_as_file(zipfile, path, mode='r'):
    """Given a ZipFile object, retrieve one of its members as a filelike.
    """
    return BytesIO(zipfile.read(path))


def get_zip_member_update_date(zipfile, path, mode='r'):
    """Given a ZipFile object and member name, retrieve the member's
    timestamp as a date.
    """
    (year, month, date, h, m, s) = zipfile.getinfo(path).date_time
    return datetime.date(year, month, date)


# src: https://stackoverflow.com/questions/45448994/wait-page-to-load-before-getting-data-with-requests-get-in-python-3
def url_to_soup_with_selenium(url, wait_conditions=None, timeout=10):
    """Some site makes multiple requests to load the data.  Calling the
    url alone doesn't return the rendered page.  As a result, selenium
    is being used to allow for the multiple requests to finish.

    """
    # TODO: make this work with get_cached_url?
    options = selenium.webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    driver = selenium.webdriver.Chrome(chrome_options=options)
    driver.get(url)
    if wait_conditions:
        try:
            wait_for_conditions_on_webdriver(driver, wait_conditions, timeout)
        except TimeoutException:
            _logger.error('Timed out waiting for element in URL: %s' % url)
            raise
    return BeautifulSoup(driver.page_source, 'lxml')


def wait_for_conditions_on_webdriver(driver, wait_conditions, timeout=10):
    try:
        conditions = [expected_conditions.presence_of_element_located(wc)
                      for wc in wait_conditions]
        for c in conditions:
            WebDriverWait(driver, timeout).until(c)
    except TimeoutException:
        _logger.error(
            'Waiting for element to load timed out in %s seconds' % timeout)
        raise


def get_session_id_from_seleniumwire(driver):
    responses = [r.response for r in driver.requests if r.response]
    response_headers = [r.headers for r in responses]
    return next((h.get('X-Session-Id') for h in response_headers if 'X-Session-Id' in h), None)
