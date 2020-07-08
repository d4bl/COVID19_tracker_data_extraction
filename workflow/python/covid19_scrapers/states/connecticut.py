import logging
import pydash
from datetime import datetime

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.http import get_json

_logger = logging.getLogger(__name__)

class Connecticut(ScraperBase):
    """
        CT makes their demographic breakdowns available as JSON at these URLs as stated in the website https://portal.ct.gov/Coronavirus/COVID-19-Data-Tracker
        
        Totals: https://data.ct.gov/resource/rf3k-f8fg.json
        Race/Ethnicity: https://data.ct.gov/resource/7rne-efic.json

        The above website also provides a link to download a daily pdf that has data embedded which is much harder to parse and needs OCR scraping as well 
        since the race/ethnicity data is embedded as images. There is code which did this parsing from PDF here, 
        https://github.com/d4bl/COVID19_tracker_data_extraction/pull/41/commits/bea25c323b52eb01c13db4f5bed1a578b9fa7937
        If at all if its useful to go back to this at some point for any reason.
    """

    CASE_DATA_URL = 'https://data.ct.gov/resource/rf3k-f8fg.json'
    RACE_DATA_URL = 'https://data.ct.gov/resource/7rne-efic.json'
    #The index (zero based) of the bar corresponding to the NH black data
    NH_BLACK_INDEX = 3
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        print('running connecticut')

    
    def _scrape(self, refresh=False, **kwargs):
        _logger.debug('Get case totals data')
        totals_json = get_json(self.CASE_DATA_URL)
        assert totals_json, 'Error finding total cases and deaths'
        
        most_recent_totals = totals_json[0]
        #dict.get sets value to None if key not availale
        report_date = datetime.strptime(most_recent_totals.get('date'), "%Y-%m-%dT%H:%M:%S.%f").date()
        total_cases = most_recent_totals.get('confirmedcases')
        total_deaths = most_recent_totals.get('confirmeddeaths') 

        assert total_cases, 'Error finding total cases'
        assert total_deaths, 'Error finding total deaths'

        #convert from string to int
        total_cases = int(total_cases)
        total_deaths = int(total_deaths)

        _logger.debug('Get race data')
        race_json = get_json(self.RACE_DATA_URL)
        assert race_json, 'Error getting race cases and deaths json'

        most_recent_nh_black_data = pydash.find(race_json, lambda data: data['hisp_race'] == 'NH Black')
        assert most_recent_nh_black_data, 'Error finding total NH Black entry'
        aa_cases = most_recent_nh_black_data.get('case_tot')
        aa_deaths = most_recent_nh_black_data.get('deaths')

        assert aa_cases, 'Error finding total NH Black cases'
        assert aa_deaths, 'Error finding total NH Black deaths'

        #convert from string to int
        aa_cases = int(aa_cases)
        aa_deaths = int(aa_deaths)
        
        pct_aa_cases = to_percentage(aa_cases, total_cases)
        pct_aa_deaths = to_percentage(aa_deaths, total_deaths)

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