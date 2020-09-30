import logging

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import query_geoservice
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Arkansas(ScraperBase):
    """Arkansas has an ArcGIS dashboard at
    https://experience.arcgis.com/experience/c2ef4a4fcbe5458fbf2e48a21e4fece9

    We call the underlying FeatureServer to populate our data.
    """
    DEMOG = dict(
        flc_url='https://services.arcgis.com/PwY9ZuZRDiI5nXUB/arcgis/rest/services/UPDATED_ADH_COVID19_STATE_METRICS/FeatureServer',
        layer_name='ADH_COVID19_TESTING_STATS_ST',
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        date, demog = query_geoservice(**self.DEMOG)
        _logger.info(f'Processing data for {date}')

        total_cases = demog.loc[0, 'positives']
        known_cases = total_cases - demog.loc[0, 'unk_race']
        aa_cases = demog.loc[0, 'black']
        pct_aa_cases = to_percentage(aa_cases, known_cases)

        total_deaths = demog.loc[0, 'deaths']
        known_deaths = total_deaths - demog.loc[0, 'd_unk_race']
        aa_deaths = demog.loc[0, 'd_black']
        pct_aa_deaths = to_percentage(aa_deaths, known_deaths)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
