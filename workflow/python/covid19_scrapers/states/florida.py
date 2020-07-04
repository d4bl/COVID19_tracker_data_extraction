import datetime
from io import BytesIO
import logging
import re

import fitz
from tabula import read_pdf
from urllib.parse import urljoin

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import url_to_soup
from covid19_scrapers.utils.http import get_content
from covid19_scrapers.utils.misc import as_list

# Backwards compatibility for datetime_fromisoformat for Python 3.6 and below
# Has no effect for Python 3.7 and above
# Reference: https://pypi.org/project/backports-datetime-fromisoformat/
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()


_logger = logging.getLogger(__name__)


def get_daily_url(reporting_url):
    """Fetch the main reporting URL and search for the latest PDF.

    """
    disaster_covid_soup = url_to_soup(reporting_url)
    find_txt = 'COVID-19 Data - Daily Report'
    daily_url = disaster_covid_soup.find(
        lambda tag: tag.has_attr('href') and re.search(find_txt, tag.text)
    ).get('href')

    if not daily_url:
        raise ValueError('Unable to find Daily Report Archive link')
    # daily report URL is often relative. urljoin fixes this.
    return urljoin(reporting_url, daily_url)


def get_report_date(url):
    match = re.search(r'(202\d)(\d\d)(\d\d)', url)
    if match:
        year, month, day = map(int, match.groups())
    else:
        match = re.search(r'latest_(\d\d)_(\d\d)', url)
        if match:
            year = datetime.datetime.now().date().year
            month, day = map(int, match.groups())
    return datetime.date(year, month, day)


def get_table_area(pdf_data):
    """This finds a bounding box for the Race, Ethnicity table by looking
    for bounding boxes for the words "White" and "Total" (occuring
    below it) on page 3 of the PDF, and the page's right bound.

    """
    doc = fitz.Document(stream=pdf_data, filetype='pdf')
    page3 = doc[2]  # page indexes start at 0

    white_bbox = None
    for (
            x0, y0, x1, y1, word, block_no, line_no, word_no
    ) in page3.getText('words'):
        if word == 'White':
            white_bbox = fitz.Rect(x0, y0, x1, y1)

    total_bbox = None
    for (
            x0, y0, x1, y1, word, block_no, line_no, word_no
    ) in page3.getText('words'):
        if word == 'Total':
            if (
                    round(x0) == round(white_bbox.x0)
                    and round(y0) > round(white_bbox.y0)
            ):
                total_bbox = fitz.Rect(x0, y0, x1, y1)

    return fitz.Rect(white_bbox.x0, white_bbox.y0,
                     page3.bound().x1, total_bbox.y1)


# Original parsers for Florida tables
def parse_num(val):
    if val:
        return float(val.replace(',', ''))
    return float('nan')


def parse_pct(val):
    if val:
        return float(val[:-1]) / 100
    return float('nan')


COLUMN_NAMES = [
    'Race/ethnicity',
    'Cases', '% Cases',
    'Hospitalizations', '% Hospitalizations',
    'Deaths', '% Deaths'
]

CONVERTERS = {
    'Cases': parse_num,
    'Hospitalizations': parse_num,
    'Deaths': parse_num,
    '% Cases': parse_pct,
    '% Hospitalizations': parse_pct,
    '% Deaths': parse_pct,
}


class Florida(ScraperBase):
    """Florida publishes a new PDF file every day containing updated
    COVID-19 statistics. We find its URL by scraping the main page.
    The file name contains the update date, and the PDF contains the
    table on page 3.

    TODO: the PDF contains cumulative case-level data, so it has
    gotten extremely large (92MB as of 26 June). We should investigate
    whether we can load it in a streaming fashion, eg if there is a
    streaming parser for linearized PDFs in python.
    """

    REPORTING_URL = 'https://floridadisaster.org/covid19/'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, refresh=False, **kwargs):
        """Set refresh to true to ignore the cache.  If false, we will still
        use conditional GET to invalidate cached data.
        """
        _logger.debug('Find daily Florida URL')
        daily_url = get_daily_url(self.REPORTING_URL)
        _logger.debug(f'URL: is {daily_url}')

        report_date = get_report_date(daily_url)
        _logger.info(f'Processing data for {report_date}')

        _logger.debug('Download the daily Florida URL')
        pdf_data = get_content(daily_url, force_remote=refresh)

        _logger.debug('Find the table area coordinates')
        table_bbox = get_table_area(pdf_data)
        table_area = (table_bbox.y0, table_bbox.x0,
                      table_bbox.y1, table_bbox.x1)

        _logger.debug('Parse the PDF')
        table = as_list(
            read_pdf(BytesIO(pdf_data),
                     pages='3',
                     stream=True,
                     multiple_tables=False,
                     area=table_area,
                     pandas_options=dict(
                         header=None,
                         names=COLUMN_NAMES,
                         converters=CONVERTERS)))[0]

        _logger.debug('Set the race/ethnicity indices')
        races = ('White', 'Black', 'Other', 'Unknown race', 'Total')
        for idx, row in table.iterrows():
            if row['Race/ethnicity'] in races:
                race = row['Race/ethnicity']
                ethnicity = 'All ethnicities'
            else:
                ethnicity = row['Race/ethnicity']
            table.loc[idx, 'Race'] = race
            table.loc[idx, 'Ethnicity'] = ethnicity

        table = table.drop('Race/ethnicity', axis=1)
        table = table.set_index(['Race', 'Ethnicity'])

        _logger.debug('Fill NAs with 1')
        table.loc[('Total', 'All ethnicities')] = table.loc[
            ('Total', 'All ethnicities')
        ].fillna(1)

        att_names = ['Cases', 'Deaths']
        all_cases_and_deaths = {nm: int(table.query(
            "Race == 'Total' and Ethnicity == 'All ethnicities'"
        )[nm].to_list()[0]) for nm in att_names}
        aa_cases_and_deaths = {nm: int(table.query(
            "Race == 'Black' and Ethnicity == 'Non-Hispanic'"
        )[nm].to_list()[0]) for nm in att_names}
        aa_cases_and_deaths_pct = {
            nm: round(100 * aa_cases_and_deaths[nm]
                      / all_cases_and_deaths[nm], 2)
            for nm in att_names
        }

        return [self._make_series(
            date=report_date,
            cases=all_cases_and_deaths['Cases'],
            deaths=all_cases_and_deaths['Deaths'],
            aa_cases=aa_cases_and_deaths['Cases'],
            aa_deaths=aa_cases_and_deaths['Deaths'],
            pct_aa_cases=aa_cases_and_deaths_pct['Cases'],
            pct_aa_deaths=aa_cases_and_deaths_pct['Deaths'],
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
