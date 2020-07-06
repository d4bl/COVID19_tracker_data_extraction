from covid19_scrapers.utils.html import url_to_soup
from covid19_scrapers.utils.http import get_content
from covid19_scrapers.utils.misc import as_list
from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage

import numpy as np
import cv2
import fitz
from urllib.parse import urljoin
from PIL import Image
import pytesseract

import datetime
import logging
import re

# Backwards compatibility for datetime_fromisoformat for Python 3.6 and below
# Has no effect for Python 3.7 and above
# Reference: https://pypi.org/project/backports-datetime-fromisoformat/
from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()


_logger = logging.getLogger(__name__)

#TODO: move this into utils
def get_daily_url(reporting_url, find_txt):
    """Fetch the main reporting URL and search for the latest PDF.

    """
    disaster_covid_soup = url_to_soup(reporting_url)
    daily_url = disaster_covid_soup.find(
        lambda tag: tag.has_attr('href') and re.search(find_txt, tag.text)
    ).get('href')

    if not daily_url:
        raise ValueError('Unable to find Daily Report Archive link')
    # daily report URL is often relative. urljoin fixes this.
    return urljoin(reporting_url, daily_url)

#TODO: move this into utils
def get_report_date(url):
    match = re.search(r'(\d+)(\d\d)(202\d)', url)
    if match:
        month, day, year = map(int, match.groups())
    else:
        match = re.search(r'latest_(\d\d)_(\d\d)', url)
        if match:
            year = datetime.datetime.now().date().year
            month, day = map(int, match.groups())
    return datetime.date(year, month, day)

def image_preprocessing(img_file, blur_type = 'bilateral', param_val = None):    
    # Read the image in with cv2, in grayscale
    img = cv2.imread(img_file)
    gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)

    #crop to get only the graph and numbers. This is an easier way to remove all the 'noise'
    #surrounding the bar chart.
    start_y = 80
    end_y = 250
    start_x = 70
    end_x = 1570

    gray = gray[start_y:end_y, start_x:end_x]
    
    # Enlarge image (make it 10x height and 10x width)
    gray = cv2.resize(gray, None, fx=6, fy=6, interpolation=cv2.INTER_CUBIC)

    # apply threshold to further make the image black and white removing other noisy colors 
    img = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 51, 2)
    img = cv2.GaussianBlur(img, (3,3), 0)

    #Tesseract scans image from top to bottom as rows which doesn't work great for text on a bar graph especially
    #when 2 bars are right next to each other, pytesseract seems to missnterpret the numbers.
    #Hence I will split the image into 8 vertical columns and parse each number.
    #8 because there are 8 race categeories for this particular graph. though this will be hardcoded for now, I don't
    #expect this number to change often
    NUM_COLUMNS = 8
    SIZE_OF_EACH_COLUMN = 295
    GAP_BETWEEN_BARS = 25
    y_len = img.shape[0]
    x_len = img.shape[1]
    for i in range(NUM_COLUMNS):
        tmp_out_file = img_file.replace('.png', str(i)+'.png')
        shift = i*SIZE_OF_EACH_COLUMN + GAP_BETWEEN_BARS
        new_img = cv2.resize(img[0:y_len, shift:shift+SIZE_OF_EACH_COLUMN], None, fx=6, fy=6, interpolation=cv2.INTER_CUBIC)
        cv2.imwrite(tmp_out_file, new_img)

def get_nh_black_data(chart, nh_black_index=3):
    """
        Function that takes the chart for the image in the daily pdf and
        preprocesses the image and applies OCR to get the number from the bar chart
        NH_BLACK_INDEX: the index of the NH Black entry in the the bar chart. This is 
        simply the 0 based index of the bar where the NH Black data is shown
    """
    filename = "chart." + chart["ext"]
    imgout = open(filename, "wb")
    imgout.write(chart["image"])
    imgout.close()
    image_preprocessing(filename, param_val=36)
    pre_processed_image_nh_black = filename.replace('.png', str(nh_black_index)+'.png')
    custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789'
    text = pytesseract.image_to_string(Image.open(pre_processed_image_nh_black), lang='eng', config=custom_config)

    return text

def get_nh_black_cases_deaths(pdf_data, NH_BLACK_INDEX):
    """This finds a bounding box for the Race, Ethnicity tables embedded as images in the pdf

    """
    doc = fitz.Document(stream=pdf_data, filetype='pdf')

    race_page = None
    #find the page that contains the race/ethnicity data
    page_begin_index = 9
    word_index = 4
    for page in doc:
        words = page.getText("words")
        #Race/Ethnicity data is on the APPENDIX B. page.
        if words[page_begin_index][word_index] == 'APPENDIX' and words[page_begin_index+1][word_index] == 'B.':
            race_page = page
            break

    if not race_page:
        _logger.error('Error getting the race/ethnicity data page')
        return

    images = race_page.getImageList()
    #the first image on the page is the cases image
    cases_xref = images[0][0]
    deaths_xref = images[1][0]
    cases_chart = doc.extractImage(cases_xref)
    deaths_chart = doc.extractImage(deaths_xref)
    cases = get_nh_black_data(cases_chart, nh_black_index=NH_BLACK_INDEX)
    deaths = get_nh_black_data(deaths_chart, nh_black_index=NH_BLACK_INDEX)

    #TODO: cleanup image files

    return (int(cases), int(deaths))

def get_total_cases_deaths(pdf_data):
    total_cases = None
    total_deaths = None
    doc = fitz.Document(stream=pdf_data, filetype='pdf')
    page1 = doc[0]  # page indexes start at 0

    page1_words = page1.getText('words')
    #Parse through 2 words at a time until we get to the right postion to parse the cases and death numbers
    #Since this info is present in the first page pretty much at the beginning this should be a concern w.r.t 
    #performance for now
    for i in range(len(page1_words)-1):
        #index 4 (zero based) has the actual word
        (word1, word2) = page1_words[i][4], page1_words[i+1][4]
        if word1 == 'COVID-19' and word2 ==  'Cases':
            #extract the cases number
            total_cases = page1_words[i+2][4]
        if word1 == 'COVID-19-Associated' and word2 == 'Deaths':
            #extract the deaths number
            total_deaths = page1_words[i+2][4]
        #if we have extracted both total_cases and total_deaths break out of the loop
        if total_cases and total_deaths:
            break
    
    return (int(total_cases), int(total_deaths))


class Connecticut(ScraperBase):
    """Connecticut publishes a new PDF file every day containing updated
    COVID-19 statistics. We find its URL by scraping the main page.
    
    The file name contains the update date, and the PDF contains the total cases
    and deaths numbers on the first page that we extract via pymupdf and the
    AA cases and deaths are embedded in an image in a subsequent page which
    are preprocessed using opencv and OCR is applied using pytesseract to extract
    the numbers.

    TODO: 
    1. should only confirmed cases and deaths be included? currently the totals have probable numbers included
    2. move certain helper functions used here into utils since there's similar usage for florida and possibly
    others
    3. cleanup .png files
    """

    REPORTING_URL = 'https://portal.ct.gov/Coronavirus/COVID-19-Data-Tracker'
    #The index (zero based) of the bar corresponding to the NH black data
    NH_BLACK_INDEX = 3
    
    def __init__(self, **kwargs):
        self.api_key = kwargs.get('google_api_key')
        super().__init__(**kwargs)
        print('running connecticut')

    
    def _scrape(self, refresh=False, **kwargs):
        """Set refresh to true to ignore the cache.  If false, we will still
        use conditional GET to invalidate cached data.
        """
        _logger.debug('Find daily Connecticut URL')
        find_txt = 'Daily Data Report'
        daily_url = get_daily_url(self.REPORTING_URL, find_txt)
        _logger.debug(f'URL: is {daily_url}')

        report_date = get_report_date(daily_url)
        _logger.info(f'Processing data for {report_date}')

        _logger.debug('Download the daily Connecticut URL')
        pdf_data = get_content(daily_url, force_remote=refresh)

        _logger.debug('Finding total cases and deaths')
        (total_cases, total_deaths) = get_total_cases_deaths(pdf_data)
        if not total_cases or not total_deaths:
            _logger.error('Error finding total cases and deaths')

        _logger.debug('Finding cases and deaths by race/ethnicity')
        (aa_cases, aa_deaths) = get_nh_black_cases_deaths(pdf_data, self.NH_BLACK_INDEX)
        if not aa_cases or not aa_deaths:
            _logger.error('Error finding total NH Black cases and deaths')
        
        pct_aa_cases = to_percentage(aa_cases, total_cases)
        pct_aa_deaths = to_percentage(aa_deaths, total_deaths)

        #TODO: should only confirmed cases and deaths be included? currently the totals have probable numbers included

        return [self._make_series(
            date=report_date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False
        )]