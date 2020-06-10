# Misc utilities
import datetime
import hashlib
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


# Content helpers.

def url_to_soup(data_url, force_remote=False):
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
        data_page = get_cached_url(data_url, force_remote=force_remote)
    except requests.RequestException:
        _logger.warn(f'request failed: {data_url}')
        raise
    _logger.debug(f'request successful: {data_url}')

    # Create a Beautiful Soup object
    data_text = data_page.text
    data_soup = BeautifulSoup(data_text, 'html.parser')

    return data_soup


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


def get_esri_metadata_date(metadata_url, force_remote=False):
    """For states using ESRI web services, the field metadata includes a
    timestamp.  This function fetches, extracts, and parses it,
    returning a datetime.date.

    It can raise get_json's exceptions, or OverflowError if the timestamp
    is not a valid date.

    """
    metadata = get_json(metadata_url, force_remote=force_remote)
    last_edit_ms = metadata['editingInfo']['lastEditDate']
    return datetime.date.fromtimestamp(last_edit_ms / 1000)


def get_esri_feature_data(data_url, fields=None, index=None,
                          force_remote=False):
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
    data = get_json(data_url, force_remote=force_remote)
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
def get_cached_url(url, local_file_name=None, force_remote=False):
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
        r = requests.get(url)
        # fall though to response handling code
    elif not local_file.exists():
        _logger.debug(f'cache file does not exist; requesting url')
        r = requests.get(url)
        # fall though to response handling code
    else:
        _logger.debug(f'Trying conditional GET')
        # Setting the If-Modified-Since header on GET turns it into a
        # Conditional GET. For details, see
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests
        mtime = local_file.stat().st_mtime
        local_file_update_time = eut.formatdate(mtime, usegmt=True)
        r = requests.get(url, headers={
            'If-Modified-Since': local_file_update_time
        })
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
                if local_file.suffix == 'html' or local_file.suffix == 'xml':
                    encoding = EncodingDetector.find_declared_encoding(
                        r.content, is_html=local_file.suffix == 'html')
                    r.encoding = encoding or 'utf-8'
                return r
    # response handling code
    r.raise_for_status()
    try:
        if local_file.parent and not local_file.parent.exists():
            _logger.debug(f'Making {local_file.parent}')
            os.makedirs(str(local_file.parent), exist_ok=True)
        with local_file.open('wb') as f:
            f.write(r.content)
            _logger.debug(f'Saved download as: {local_file}')
    except OSError as e:
        _logger.warn(f'Saving to cache failed: {local_file}: {e}')

    return r


def download_file(file_url, new_file_name=None, force_remote=False):
    """Save the url contents in the specified file."""
    try:
        get_cached_url(file_url, local_file_name=new_file_name,
                       force_remote=force_remote)
    except Exception as e:
        _logger.warn(f'File download failed: {new_file_name}: {e}')
        raise


def get_json(url, force_remote=False):
    """Return the url's reponse contents as parsed JSON.

    This can raise a requests.RequestException if retrieval fails, or
    a ValueError if the JSON cannot be decoded.
    """
    # The next line can raise a requests.RequestException
    r = get_cached_url(url, force_remote=force_remote)
    # The next line can raise a ValueError
    return r.json()


def get_content(url, force_remote=False):
    """Return the url's reponse contents as bytes."""
    r = get_cached_url(url, force_remote=force_remote)
    return r.content


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
