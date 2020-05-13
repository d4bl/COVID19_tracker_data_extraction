from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger('covid19_scrapers')


class Georgia(ScraperBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def _scrape(self, validation):
        _logger.debug('Download file')
        r = requests.get('https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip')

        _logger.debug('Read contents of the zip')
        z = zipfile.ZipFile(BytesIO(r.content))

        _logger.debug('Report date = last update of the demographics.csv file in the ZIP archive')
        info = z.getinfo('demographics.csv')
        zip_date = datetime.date(*info.date_time[0:3])
        zip_date_fmt = zip_date.strftime('%m/%d/%Y')

        _logger.debug('Load demographics CSV')
        with z.open('demographics.csv') as cases:
            data = pd.read_csv(cases)
        by_race = data[['race', 'Confirmed_Cases', 'Deaths']].groupby('race').sum()
        totals = by_race.sum(axis=0)

        _logger.debug('African American cases and deaths')
        ga_aa_cases_pct = round(100 * by_race.loc['AFRICAN-AMERICAN', 'Confirmed_Cases'] / totals['Confirmed_Cases'], 2)
        ga_aa_deaths_pct = round(100 * by_race.loc['AFRICAN-AMERICAN', 'Deaths'] / totals['Deaths'], 2)
        
        return [self._make_series(
            date=zip_date_fmt,
            cases=totals['Confirmed_Cases'],
            deaths=totals['Deaths'],
            aa_cases=by_race.loc['AFRICAN-AMERICAN', 'Confirmed_Cases'],
            aa_deaths=by_race.loc['AFRICAN-AMERICAN', 'Deaths'],
            pct_aa_cases=ga_aa_cases_pct,
            pct_aa_deaths=ga_aa_deaths_pct,
        )]
