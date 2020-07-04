import logging
import re

from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import pandas as pd
import requests

from covid19_scrapers.utils.http import get_cached_url
from covid19_scrapers.utils.parse import maybe_convert


_logger = logging.getLogger(__name__)


# HTML content helpers.
def url_to_soup(data_url, **kwargs):
    """
    Retrieve parsed web page from specified URL.

    Parameters
    ----------
    data_url: string
        website link

    Returns a Beautifulsoup object representing the HTML code from webpage.
    """
    try:
        data_page = get_cached_url(data_url, **kwargs)
    except requests.RequestException:
        _logger.warning(f'request failed: {data_url}')
        raise
    _logger.debug(f'request successful: {data_url}')

    # Create a Beautiful Soup object
    data_text = data_page.text
    data_soup = BeautifulSoup(data_text, 'html.parser')

    return data_soup


def find_all_links(url, search_string=None, links_and_text=False):
    """Extract the matching links from the specified web page.

    Based on the answer at
    https://stackoverflow.com/questions/1080411/retrieve-links-from-web-page-using-python-and-beautifulsoup

    It prefers the in-document declaration (or detection) of character
    set encoding to one in the HTTP header, if any.

    Arguments:
      url: the web page to retrieve
      search_string: if present, a substring or regexp to filter URLs
        to return.
      links_and_text: indicates that the caller wants both links and text.

    Returns a list of the URLs in the document if links_and_text is
    False, or a dict mapping URLs to their anchor text if
    links_and_text is True.

    """
    resp = get_cached_url(url)
    http_encoding = resp.encoding if 'charset' in resp.headers.get(
        'content-type', '').lower() else None
    html_encoding = EncodingDetector.find_declared_encoding(resp.content,
                                                            is_html=True)
    encoding = html_encoding or http_encoding
    soup = BeautifulSoup(resp.content, from_encoding=encoding,
                         features='lxml')

    title_dict = dict()

    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.text
        if search_string:
            if isinstance(search_string, str):
                if search_string in href:
                    title_dict[href] = text
            elif isinstance(search_string, re.Pattern):
                if search_string.search(href):
                    title_dict[href] = text
        else:
            title_dict[href] = text

    if links_and_text:
        return title_dict
    else:
        return list(title_dict.keys())


def table_to_dataframe(table):
    """Make a DataFrame from a BeautifulSoup `table` element.

    Returns a DataFrame whose columns are the `th` contents if
    present, otherwise the first row's `td` contents, and whose data
    are the remaining `td` items converted using maybe_convert.

    """
    ths = table.find_all('th')
    trs = table.find_all('tr')
    if ths:
        columns = [th.text.strip() for th in ths]
    else:
        columns = [td.text.strip() for td in trs[0]]
        trs = trs[1:]
    _logger.debug(f'Creating DataFrame with columns {columns}')

    data = [[maybe_convert(td.text)
             for idx, td in enumerate(tr.find_all('td'))
             if idx < len(columns)]
            for tr in trs]
    return pd.DataFrame(data, columns=columns).dropna(how='all')
