# Misc utilities
from arcgis.gis import GIS
import datetime
import hashlib
import logging
import pandas as pd
from pathlib import Path
import os
import re
import time
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


ssl._create_default_https_context = ssl._create_unverified_context
_logger = logging.getLogger(__name__)


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


def make_geoservice_stat(agg, in_field, out_name):
    return {
        'statisticType': agg,
        'onStatisticField': in_field,
        'outStatisticFieldName': out_name or in_field,
    }


def make_geoservice_args(
        *, flc_id, layer_name,
        where=None, out_fields=None,
        group_by=None, stats=None,
        order_by=None, limit=None):
    ret = {
        'flc_id': flc_id,
        'layer_name': layer_name,
    }
    if where is not None:
        ret['where'] = where
    if out_fields is not None:
        ret['out_fields'] = out_fields
    if group_by is not None:
        ret['group_by'] = group_by
    if stats is not None:
        ret['stats'] = stats
    if order_by is not None:
        ret['order_by'] = order_by
    if limit is not None:
        ret['limit'] = limit
    return ret


def query_geoservice(flc_id, layer_name, *,
                     where='1=1', out_fields=['*'],
                     group_by=None, stats=None,
                     order_by=None, limit=None):
    """Queries the specified ESRI GeoService.

    Positional arguments:
      flc_id: FeatureLayerCollection ID to search for.
      layer_name: the name of the desired layer in the FeatureLayerCollection.

    Keyword arguments:
      where: the feature filtering query.
      out_fields: the fields to retrieve, defaults to all.
      group_by: the field by which to group for statistical operations.
      stats: a list of dicts specifying the desired statistical operations.
      order_by: field and direction to order by.
      limit: max number of records to retrieve.

    Returns: a pair consisting of the update date and data frame
      containing the features.
    """

    gis = GIS()
    flc = gis.content.search(f'id:{flc_id}')[0]
    layers = [layer
              for layer in flc.layers
              if layer.properties.name == layer_name]
    if layers:
        layer = layers[0]
    else:
        tables = [table
                  for table in flc.tables
                  if table.properties.name == layer_name]
        if tables:
            layer = tables[0]
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
    update_date = datetime.datetime.fromtimestamp(
        layer.properties.editingInfo.lastEditDate/1000).date()
    return update_date, features.sdf


def get_esri_metadata_date(metadata_url, **kwargs):
    """For states using ESRI web services, the field metadata includes a
    timestamp.  This function fetches, extracts, and parses it,
    returning a datetime.date.

    It can raise get_json's exceptions, or OverflowError if the timestamp
    is not a valid date.

    """
    metadata = get_json(metadata_url, **kwargs)
    last_edit_ms = metadata['editingInfo']['lastEditDate']
    return datetime.date.fromtimestamp(last_edit_ms / 1000)


def get_esri_feature_data(data_url, fields=None, index=None,
                          **kwargs):
    """For states using ESRI web services, the feature data includes a
    list of fields and their values.

    This function
    * fetches, extracts, and parses the feature response,
    * ensures that any requested fields are present,
    * finally returns a DataFrame of the features' attribute objects.

    It can raise any of get_json's exceptions, KeyError if a required
    JSON field is missing, and ValueError if requested fields are not
    returned by the API.

    """
    data = get_json(data_url, **kwargs)
    # Validate fields
    if fields:
        valid_fields = set(field['name'] for field in data['fields'])
        extra_fields = list(set(fields) - valid_fields)
        if extra_fields:
            raise ValueError('Requested fields not present in API ' +
                             f'response: {extra_fields}')

    ret = pd.DataFrame(
        [feature['attributes'] for feature in data['features']])
    if index:
        ret = ret.set_index(index)
    return ret


# Helpers for HTTP data retrieval.
def _send_request(*, session, method, url, headers, cookies, params,
                  data, files):
    req = requests.Request(method, url=url, headers=headers,
                           cookies=cookies, params=params, data=data,
                           files=files)
    preq = session.prepare_request(req)
    return session.send(preq)


def get_cached_url(url, local_file_name=None, force_remote=False,
                   method='GET', headers={}, params={}, data={},
                   files={}, cookies={}, session=None,
                   session_args={}, **kwargs):
    """Retrieve a URL and cache the results in a local file.

    force_remote: inhibits checking the cached file.

    local_file_name: string or None.  A default value is generated
      from the URL if not provided.

    If the local file exists, its modification time is used in a
    conditional GET for url, and if a newer version is available, it
    replaces the cached data.  If the local file is up-to-date, its
    contents are used.

    Returns a requests.Response object.
    """
    if session is None:
        session = requests.Session(**session_args)

    if local_file_name:
        local_file = Path(local_file_name)
    else:
        url_parts = urlsplit(url)
        if url_parts.query:
            local_file = Path('./' + url_parts.path + '_' +
                              hashlib.md5(
                                  url_parts.query.encode('utf-8')
                              ).hexdigest())
        else:
            local_file = Path('./' + url_parts.path)
    _logger.debug(f'Using local file {local_file}')
    r = None
    if force_remote:
        _logger.debug(f'force_remote is set; requesting url')
        # fall though to response handling code
    elif method != 'GET':
        _logger.debug(f'Cache requires conditional GET, but requested method is {method}; requesting url')
        # fall though to response handling code
    elif not local_file.exists():
        _logger.debug(f'cache file does not exist; requesting url')
        # fall though to response handling code
    else:
        _logger.debug(f'Trying conditional GET')
        # Setting the If-Modified-Since header on GET turns it into a
        # Conditional GET. For details, see
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests
        mtime = local_file.stat().st_mtime
        local_file_update_time = eut.formatdate(mtime, usegmt=True)
        cond_headers = {'If-Modified-Since': local_file_update_time}
        cond_headers.update(headers)
        r = _send_request(session=session, method=method, url=url,
                          params=params, data=data,
                          files=files, headers=cond_headers,
                          cookies=cookies)
        # A status of 304 means "Not modified"
        if r.status_code != 304:
            _logger.debug('Cached file is stale: ' +
                          f'last-fetch: {local_file_update_time}, ' +
                          f'current-fetch: {r.headers.get("Last-Modified")}')
            # fall though to response handling code
        else:
            _logger.info(f'File cache hit: {local_file.name}')
            with local_file.open('rb') as f:
                r = requests.Response()
                r.url = url
                r.status_code = 200
                r._content = f.read()
                r._content_consumed = True
                r.headers['last-modified'] = eut.format_datetime(
                    datetime.datetime.fromtimestamp(mtime))
                if local_file.suffix == 'html' or local_file.suffix == 'xml':
                    encoding = EncodingDetector.find_declared_encoding(
                        r.content, is_html=local_file.suffix == 'html')
                    r.encoding = encoding or 'utf-8'
                return r
    # non-cache retrieval cases
    if r is None:
        r = _send_request(session=session, method=method, url=url,
                          params=params, data=data, files=files,
                          headers=headers, cookies=cookies)
    # response handling code
    r.raise_for_status()
    last_modified = r.headers.get('last-modified')
    if last_modified:
        mtime = eut.parsedate_to_datetime(last_modified).timestamp()
    else:
        mtime = time.time()
    try:
        if local_file.parent and not local_file.parent.exists():
            _logger.debug(f'Making {local_file.parent}')
            os.makedirs(str(local_file.parent), exist_ok=True)
        with local_file.open('wb') as f:
            f.write(r.content)
            _logger.debug(f'Saved download as: {local_file}')
        os.utime(local_file, (mtime, mtime))
    except OSError as e:
        _logger.warn(f'Saving to cache failed: {local_file}: {e}')

    return r


def download_file(file_url, new_file_name=None, **kwargs):
    """Save the url contents in the specified file."""
    try:
        get_cached_url(file_url, local_file_name=new_file_name,
                       **kwargs)
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
    driver = selenium.webdriver.Chrome(chrome_options=options)
    driver.get(url)
    if wait_conditions:
        try:
            conditions = [expected_conditions.presence_of_element_located(wc)
                          for wc in wait_conditions]
            for c in conditions:
                WebDriverWait(driver, timeout).until(c)
        except TimeoutException:
            _logger.error('Waiting for element to load timed out in %s seconds for url: %s' % (timeout, url))
            raise
    return BeautifulSoup(driver.page_source, 'lxml')
