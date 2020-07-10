import datetime
import logging
import re

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.html import table_to_dataframe, url_to_soup
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


def _maybe_int(val):
    try:
        return int(val)
    except ValueError:
        return val


class Washington(ScraperBase):
    """Washington (state) has a reporting page that does some odd tricks
    (see HACK ALERT comment below). We replicate those tricks to get
    the report HTML source, and extract the desired data from that.
    """

    DATA_URL = 'https://www.doh.wa.gov/Emergencies/Coronavirus'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # HACK ALERT:
        # The main page dynamically adds JavaScript to insert and
        # submit (POST) a form with the field "_pd" set. The POST body
        # most be mime type multipart/form-data rather than the
        # requests default application/x-www-form-urlencoded.  We can
        # make requests generate this by using the files argument
        # instead of data for the form data. Using a file name key of
        # None prevents the extraneous name from being included in the
        # call.
        soup = url_to_soup(self.DATA_URL, method='POST', files={None: b'_pd'})

        # Find the update date
        last_updated_text = soup.find('strong', string=re.compile('Last Updated'))
        month, day, year = map(
            int,
            re.search(
                r'(\d\d)/(\d\d)/(\d\d\d\d)',
                last_updated_text.find_next('strong').string
            ).groups())
        date = datetime.date(year, month, day)
        _logger.info(f'Processing data for {date}')

        # Load the cases by race/ethnicity table
        cases_div = soup.find(id='pnlConfirmedCasesByRaceTbl')
        cases = table_to_dataframe(
            cases_div.find('table')).set_index('Race/Ethnicity')
        # Fix column names
        cases.columns = cases.columns.str.replace('\xa0.*', '')
        # Extract the data
        total_cases = cases.loc['Total Number of Cases',
                                'Confirmed Cases']
        known_cases = cases.loc['Total with Race/Ethnicity Available',
                                'Confirmed Cases']
        aa_cases = cases.loc['Non-Hispanic Black', 'Confirmed Cases']
        aa_cases_pct = to_percentage(aa_cases, known_cases)

        deaths_div = soup.find(id='pnlDeathsByRaceTbl')
        deaths = table_to_dataframe(
            deaths_div.find('table')).set_index('Race/Ethnicity')
        deaths.columns = deaths.columns.str.replace('\xa0.*', '')
        total_deaths = deaths.loc['Total Number of Deaths', 'Deaths']
        known_deaths = deaths.loc['Total with Race/Ethnicity Available',
                                  'Deaths']
        aa_deaths = deaths.loc['Non-Hispanic Black', 'Deaths']
        aa_deaths_pct = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=_maybe_int(total_cases),
            deaths=_maybe_int(total_deaths),
            aa_cases=_maybe_int(aa_cases),
            aa_deaths=_maybe_int(aa_deaths),
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
