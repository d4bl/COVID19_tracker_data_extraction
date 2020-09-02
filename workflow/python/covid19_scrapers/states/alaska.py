import logging

from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import query_geoservice
from covid19_scrapers.utils.misc import to_percentage


_logger = logging.getLogger(__name__)


class Alaska(ScraperBase):
    """Alaska publishes demographic breakdows of COVID-19 cases and deaths
    in their ArcGIS dashboard at
    https://coronavirus-response-alaska-dhss.hub.arcgis.com/
    """

    # Service is under https://services1.arcgis.com/WzFsmainVTuD5KML
    DATA = dict(
        flc_id='ebf62bbdba59497a9dba00aed0c17078',
        layer_name='Demographic_Distribution_of_Confirmed_Cases',
        out_fields=[
            'Demographic',
            'All_Cases as Cases',
            'All_Cases_Percentage as Cases_Pct',
            'Deaths',
            'Deaths_Percentage as Deaths_Pct',
        ],
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download the metadata
        date, data = query_geoservice(**self.DATA)
        _logger.info(f'Processing data for {date}')
        data = data.set_index('Demographic')

        # Discard non-race rows
        data = data.reindex(['White', 'Black', 'AI/AN', 'Asian',
                             'NHOPI', 'Multiple', 'Other', 'Unknown Race'])

        # Add total rows
        data.loc['Grand Total', :] = data.sum()
        data.loc['Known Race', :] = data.drop(['Grand Total', 'Unknown Race']).sum()

        # Extract/calculate case info
        total_cases = data.loc['Grand Total', 'Cases']
        known_cases = data.loc['Known Race', 'Cases']
        aa_cases_cnt = data.loc['Black', 'Cases']
        aa_cases_pct = to_percentage(aa_cases_cnt, known_cases, 2)

        # Extract/calculate death info
        total_deaths = data.loc['Grand Total', 'Deaths']
        known_deaths = data.loc['Known Race', 'Deaths']
        aa_deaths_cnt = data.loc['Black', 'Deaths']
        aa_deaths_pct = to_percentage(aa_deaths_cnt, known_deaths, 2)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_cases,
            known_race_deaths=known_deaths,
        )]
