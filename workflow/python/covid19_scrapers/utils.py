## Misc utilities
import logging
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import datetime
from pathlib import Path
from urllib.parse import urlsplit

## Read webpage
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import requests
from zipfile import ZipFile


# Import packages needed to run GA code
#import datetime
import email.utils as eut
from io import BytesIO
import re
import zipfile

import urllib.request
import requests
import ssl
import shutil
ssl._create_default_https_context = ssl._create_unverified_context


_logger = logging.getLogger('covid19_scrapers')


def as_list(arg):
    """If arg is a list, return it.  Otherwise, return a list containing it."""
    if isinstance(arg, list):
        return arg
    return [arg]


# Source: https://stackoverflow.com/questions/1080411/retrieve-links-from-web-page-using-python-and-beautifulsoup

def find_all_links(url, search_string=None):
    resp = requests.get(url)
    http_encoding = resp.encoding if 'charset' in resp.headers.get('content-type', '').lower() else None
    html_encoding = EncodingDetector.find_declared_encoding(resp.content, is_html=True)
    encoding = html_encoding or http_encoding
    soup = BeautifulSoup(resp.content, from_encoding=encoding, features="lxml")

    link_list = []

    for link in soup.find_all('a', href=True):
        link_list.append(link['href'])

    if search_string:
        return [x for x in link_list if search_string in x]
    else:
        return link_list


def get_cached_url(url, local_file_name=None, force_remote=False):
    """Retrieve a URL and cache the results in a local file, using that on subsequent calls if force_remote is false.
    Returns a requests.Response object.
    """
    if local_file_name:
        local_file = Path(local_file_name)
    else:
        url_parts = urlsplit(url)
        if url_parts.query:
            local_file = Path('.') / Path(url_parts.path + '_' + 
                                          url_parts.query.replace('&', '_'))
        else:
            local_file = Path('.') / Path(url_parts.path)
    r = None 
    if force_remote:
        r = requests.get(url)
        # fall though to response handling code
    elif not local_file.exists():
        r = requests.get(url)
        # fall though to response handling code
    else:
        # Setting the If-Modified-Since header on GET turns it into a Conditional GET
        # See https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests
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
        if local_file.parent and not local_file.parent.exists():
            _logger.warn(f'Creating parent dir(s): {local_file.parent}')
            os.makedirs(str(local_file.parent))
        with local_file.open('wb') as f:
            f.write(r.content)
            _logger.debug(f'Saved download as: {local_file.name}')
    return r


def download_file(file_url, new_file_name=None):
    try:
        get_cached_url(file_url, local_file_name=new_file_name)
    except Exception as e:
        _logger.warn(f'File download failed: {new_file_name}: {e}')
        raise
    
    


## Wrapper to unzip files
def unzip(path_to_zip_file, directory_to_extract_to='.'):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

def get_zip(url):
    r = get_cached_url(url)
    return ZipFile(BytesIO(r.content))

def get_zip_member_as_file(zipfile, path, mode='r'):
    return BytesIO(zipfile.read(path))

def get_zip_member_update_date(zipfile, path, mode='r'):
    (year, month, date, h, m, s) = zipfile.getinfo(path).date_time
    return datetime.date(year, month, date)

def url_to_soup(data_url):
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
        data_page = get_cached_url(data_url)
    except requests.RequestException:
        _logger.warn(f'request failed: {data_url}')
        raise
    _logger.debug(f'request successful: {data_url}')

    # Create a Beautiful Soup object
    data_text = data_page.text
    data_soup = BeautifulSoup(data_text, "html.parser")

    return data_soup



def get_json(url):
    """Simple function to return the parsed JSON from a web API."""
    # The next line can raise a requests.RequestException
    r = get_cached_url(url) 
    # The next line can raise a ValueError
    return r.json()


def get_content(url, force_remote=False):
    """Return the content of a remote URL, possibly caching in a local file."""
    r = get_cached_url(url, force_remote=False)
    return r.content


def get_metadata_date(metadata_url):
    """For states using ESRI web services, the field metadata includes a timestamp. 
    This function fetches, extracts, and parses it, returning a datetime.date.
    """
    metadata = get_json(metadata_url)
    last_edit_ms = metadata['editingInfo']['lastEditDate']
    # The next line can raise OverflowError
    return datetime.date.fromtimestamp(last_edit_ms / 1000)


















