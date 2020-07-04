from covid19_scrapers.utils import (query_geoservice, to_percentage)
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger(__name__)


class Alabama(ScraperBase):
    """Alabama has an ArcGIS dashboard that includes demographic
    breakdowns of confirmed cases and deaths.  We retrieve the data
    using the underlying FeatureServer API calls used to populate the
    dashboard.

    The dashboard is at:
    https://alpublichealth.maps.arcgis.com/apps/opsdashboard/index.html#/6d2771faa9da4a2786a509d82c8cf0f7

    """

    # Services are under https://services7.arcgis.com/4RQmZZ0yaZkGR1zy
    CASES = dict(
        flc_id='0c2185b8174646979f4abb5a45ef05c3',
        layer_name='Statewide Race',
        out_fields=['Racecat', 'Race_Counts as value'],
    )

    DEATHS = dict(
        flc_id='015fd1e0d8074840ab624243a74c54c9',
        layer_name='Race',
        out_fields=['Racecat', 'DiedFromCovid19 as value'],
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        # Download the case data
        date, cases = query_geoservice(**self.CASES)
        _logger.info(f'Processing data for {date}')
        cases = cases.set_index('Racecat')

        # Extract/calculate case info
        total_cases = cases.loc[:, 'value'].sum()
        total_known_cases = cases.loc[:, 'value'].drop('Unknown').sum()
        aa_cases_cnt = cases.loc['Black', 'value']
        aa_cases_pct = to_percentage(aa_cases_cnt, total_known_cases)

        # Download the deaths data
        _, deaths = query_geoservice(**self.DEATHS)
        deaths = deaths.set_index('Racecat')

        # Extract/calculate deaths info
        total_deaths = deaths.loc[:, 'value'].sum()
        total_known_deaths = deaths.loc[:, 'value'].drop('Unknown').sum()
        aa_deaths_cnt = deaths.loc['Black', 'value']
        aa_deaths_pct = to_percentage(aa_deaths_cnt, total_known_deaths)

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases_cnt,
            aa_deaths=aa_deaths_cnt,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=False,
            known_race_cases=total_known_cases,
            known_race_deaths=total_known_deaths,
        )]
