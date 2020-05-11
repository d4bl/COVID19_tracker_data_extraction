## Misc utilities
import logging
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import datetime

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




def download_file(file_url, new_file_name=None):
    try:
        try:
            urllib.request.urlretrieve(file_url, new_file_name)
            _logger.debug(f'File download success: {new_file_name}')
        except:
            r = requests.get(file_url)

            with open(new_file_name, 'wb') as f:
                f.write(r.content)
                
            _logger.debug(f'File download success: {new_file_name}')
    except:
        _logger.warn(f'File download failed: {new_file_name}')
    
    


## Wrapper to unzip files
def unzip(path_to_zip_file, directory_to_extract_to='.'):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

def get_zip(url):
    r = requests.get(url)
    r.raise_for_status()
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
    data_page = requests.get(data_url)
    if (data_page.status_code) == 200:
        _logger.debug(f'request successful: {data_url}')
    else:
        _logger.warn(f'request failed: {data_url}')

    # Create a Beautiful Soup object
    data_text = data_page.text
    data_soup = BeautifulSoup(data_text, "html.parser")

    return data_soup



def get_json(url):
    """Simple function to return the parsed JSON from a web API."""
    # The next two lines can raise a requests.RequestException
    r = requests.get(url) 
    r.raise_for_status()
    # The next line can raise a ValueError
    return r.json()




def get_metadata_date(metadata_url):
    """For states using ESRI web services, the field metadata includes a timestamp. 
    This function fetches, extracts, and parses it, returning a datetime.date.
    """
    metadata = get_json(metadata_url)
    last_edit_ms = metadata['editingInfo']['lastEditDate']
    # The next line can raise OverflowError
    return datetime.date.fromtimestamp(last_edit_ms / 1000)


















