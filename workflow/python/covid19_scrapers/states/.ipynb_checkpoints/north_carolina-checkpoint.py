from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger('covid19_scrapers')


class NorthCarolina(ScraperBase):
    REPORTING_URL = 'https://www.ncdhhs.gov/divisions/public-health/covid19/covid-19-nc-case-count#by-race-ethnicity'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def name(self):
        return "North Carolina"
    
    def _scrape(self, validation):
        NC_soup = url_to_soup(self.REPORTING_URL)
        
        # find date and total number of cases and deaths
        date_text = NC_soup.find("div", attrs={"class":"field-item"}).p.text[50:]
        date_time_obj = datetime.datetime.strptime(date_text, "%B %d, %Y. ")
        date_formatted = date_time_obj.strftime("%m/%d/%Y")
        
        field_item = NC_soup.find("div", attrs={"class":"field-item"})
        # num_cases = field_item.findAll("tr")[1].td.text
        thead = field_item.find("thead")
        for idx, th in enumerate(thead.tr.find_all('th')):
            if th.text.find('Cases') >= 0:
                cases_idx = idx
            elif th.text.find('Deaths') >= 0:
                deaths_idx = idx
        tbody = field_item.find("tbody")
        tds = tbody.find_all('td')
        num_cases = int(tds[cases_idx].text.replace(',', ''))
        num_deaths = int(tds[deaths_idx].text.replace(',', ''))
        
        _logger.debug(f'Date: {date_formatted}')
        _logger.debug(f'Number Cases:  {num_cases}')
        _logger.debug(f'Number Deaths: {num_deaths}')
        
        # find number of Black/AA cases and deaths
        h2 = NC_soup.find('h2', string=re.compile('Race/Ethnicity'))
        race_data = h2.find_next_sibling("table")
        thead = race_data.find("thead")
        ths = thead.find_all("th")
        _logger.debug(f'Found {len(ths)} THs')
        for idx, th in enumerate(ths):
            # Search for percentages first to avoid false matches
            text = th.text.strip()
            if text == 'Laboratory-Confirmed Cases':
                aa_cases_idx = idx
            elif text == '% Laboratory-Confirmed Cases':
                pct_aa_cases_idx = idx
            elif text == 'Deaths from COVID-19':
                aa_deaths_idx = idx
            elif text == '% Deaths from COVID-19':
                pct_aa_deaths_idx = idx
        _logger.debug(f'Processing body')
        tbody = race_data.find('tbody')
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if tds[0].text == 'Black or African American':
                cnt_aa_cases = int(tds[aa_cases_idx].text.replace(',', ''))
                cnt_aa_deaths = int(tds[aa_deaths_idx].text.replace(',', ''))
                pct_aa_cases = int(tds[pct_aa_cases_idx].text.strip('%'))
                pct_aa_deaths = int(tds[pct_aa_deaths_idx].text.strip('%'))

        _logger.debug(f'Pct Cases Black/AA: {pct_aa_cases}')
        _logger.debug(f'Pct Deaths Black/AA: {pct_aa_deaths}')

        return [self._make_series(
            date=date_formatted,
            cases=num_cases,
            deaths=num_deaths,
            aa_cases=cnt_aa_cases,
            aa_deaths=cnt_aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
        )]
