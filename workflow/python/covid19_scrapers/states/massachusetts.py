import datetime
import logging

import pandas as pd

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import find_all_links
from covid19_scrapers.utils.misc import to_percentage
from covid19_scrapers.utils.zip import get_zip, get_zip_member_as_file


_logger = logging.getLogger(__name__)


class Massachusetts(ScraperBase):
    """Massachusetts publishes ZIP files containing CSV files of COVID-19
    statistics. A new file is uploaded daily. We scrape the main
    reporting page to find the latest ZIP file link.
    """

    REPORTING_URL = 'https://www.mass.gov/info-details/covid-19-response-reporting'
    DOWNLOAD_URL_TEMPLATE = 'https://www.mass.gov/doc/{}/download'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, *, validation=False, **kwargs):
        urls = find_all_links(url=self.REPORTING_URL,
                              search_string='covid-19-raw-data')
        _logger.debug(f'Fetching links from {urls}')

        url_fragment = urls[0].split('/')[2]
        url = self.DOWNLOAD_URL_TEMPLATE.format(url_fragment)
        _logger.debug(f'Current COVID-19 data: {url}')

        # Cumulative number of cases / deaths
        ma_zip = get_zip(url)

        _logger.debug('Get the race/ethnicity breakdown')
        df_raw = pd.read_csv(
            get_zip_member_as_file(ma_zip, 'RaceEthnicity.csv'),
            parse_dates=['Date']
        )

        _logger.debug('Get date of most recent data published')
        # If desired (validation = True), verify that calculations as
        # of D4BL's last refresh match these calculations.
        if validation is True:
            max_date = datetime.date(2020, 4, 9)
        else:
            max_date = max(df_raw.Date)

        _logger.info(f'Processing data for {max_date}')
        df_mass = df_raw[df_raw.Date == max_date]

        # Intermediate calculations

        total_cases = df_mass['All Cases'].sum()
        total_deaths = df_mass['Deaths'].sum()
        aa_cases = df_mass[
            df_mass['Race/Ethnicity']
            == 'Non-Hispanic Black/African American'
        ]['All Cases'].tolist()[0]
        aa_cases_pct = to_percentage(aa_cases, total_cases)
        aa_deaths = df_mass[
            df_mass['Race/Ethnicity']
            == 'Non-Hispanic Black/African American'
        ]['Deaths'].tolist()[0]
        aa_deaths_pct = to_percentage(aa_deaths, total_deaths)
        return [self._make_series(
            date=max_date.date(),
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
