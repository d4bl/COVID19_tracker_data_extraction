from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger('covid19_scrapers')


class Massachusetts(ScraperBase):
    REPORTING_URL = 'https://www.mass.gov/info-details/covid-19-response-reporting'
    DOWNLOAD_URL_TEMPLATE = 'https://www.mass.gov/doc/{}/download'
    
    def __init__(self):
        super().__init__()
        
    def _scrape(self, validation, home_dir):
        mass_urls = find_all_links(url=self.REPORTING_URL, search_string='covid-19-raw-data')
        _logger.debug(f'Fetching links from {mass_urls}')

        mass_url_fragment = mass_urls[0].split('/')[2]
        mass_url = self.DOWNLOAD_URL_TEMPLATE.format(mass_url_fragment)
        _logger.debug(f'Current COVID-19 data: {mass_url}')
        
        ## Cumulative number of cases / deaths
        ma_zip = get_zip(mass_url)
        
        _logger.debug('Get the race/ethnicity breakdown')
        df_mass_raw = pd.read_csv(
            get_zip_member_as_file(ma_zip, 'RaceEthnicity.csv'))
        
        _logger.debug('Get date of most recent data published')
        ## If desired (validation = True), verify that calculations as of D4BL's last refresh match these calculations 
        ## TO DO: Convert date to string first before finding the max
        if validation is True:
            mass_max_date = '4/9/2020'
        else:
            mass_max_date = max(df_mass_raw.Date)
        
        _logger.debug(f'Extracting data for {mass_max_date}')
        df_mass = df_mass_raw[df_mass_raw.Date == mass_max_date]
        
        ##### Intermediate calculations #####
        
        mass_total_cases = df_mass['All Cases'].sum()
        mass_total_deaths = df_mass['Deaths'].sum()
        mass_aa_cases = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['All Cases'].tolist()[0] 
        mass_aa_cases_pct = round(100 * mass_aa_cases / mass_total_cases, 2)
        mass_aa_deaths = df_mass[df_mass['Race/Ethnicity'] == 'Non-Hispanic Black/African American']['Deaths'].tolist()[0]
        mass_aa_deaths_pct = round(100 * mass_aa_deaths / mass_total_deaths, 2)
        return [self._make_series(
            date=mass_max_date,
            cases=mass_total_cases,
            deaths=mass_total_deaths,
            aa_cases=mass_aa_cases,
            aa_deaths=mass_aa_deaths,
            pct_aa_cases=mass_aa_cases_pct,
            pct_aa_deaths=mass_aa_deaths_pct,
        )]
