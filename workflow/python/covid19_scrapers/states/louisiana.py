from covid19_scrapers.scraper import ScraperBase
from covid19_scrapers.utils.arcgis import query_geoservice, make_geoservice_stat
from covid19_scrapers.utils.misc import to_percentage


class Louisiana(ScraperBase):
    """The information for Louisiana comes from an ArcGIS dashboard.

    The associated services are commented below, above the query parameters.
    """

    # https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Cases_and_Deaths_by_Race_by_Region/FeatureServer/layers
    RACE_QUERY = dict(
        flc_id='0ee12fc733d143e0ab35a33bb0f93406',
        layer_name='Sheet3',
        out_fields=['Race', 'Deaths', 'Cases'])

    # https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Combined_COVID_Reporting/FeatureServer/layers
    TOTAL_CASES_QUERY = dict(
        flc_id='63aa4507396e4aa1ba90ee3eb5b8f05a',
        layer_name='Sheet1',
        where="Measure='Case Count' AND Group_Num<>38",
        stats=[make_geoservice_stat('sum', 'Value', 'value')])

    # https://services5.arcgis.com/O5K6bb5dZVZcTo5M/ArcGIS/rest/services/Combined_COVID_Reporting/FeatureServer/layers
    TOTAL_DEATHS_QUERY = dict(
        flc_id='63aa4507396e4aa1ba90ee3eb5b8f05a',
        layer_name='Sheet1',
        where="Measure='Deaths' AND Group_Num<>38",
        stats=[make_geoservice_stat('sum', 'Value', 'value')])

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _scrape(self, **kwargs):
        _, total_cases_df = query_geoservice(**self.TOTAL_CASES_QUERY)
        _, total_deaths_df = query_geoservice(**self.TOTAL_DEATHS_QUERY)
        date, raw_race_df = query_geoservice(**self.RACE_QUERY)
        race_df = raw_race_df.groupby('Race').agg({'Cases': 'sum', 'Deaths': 'sum'})

        assert len(total_cases_df) == 1, 'total_cases_df has unexpected number of rows'
        assert len(total_deaths_df) == 1, 'total_deaths_df has unexepected number of rows'
        assert len(race_df) == 7, 'race_df has unexpected number of rows'

        cases = total_cases_df.iloc[0]['value']
        deaths = total_deaths_df.iloc[0]['value']
        aa_cases = race_df.loc['Black']['Cases']
        aa_deaths = race_df.loc['Black']['Deaths']
        known_race_cases = race_df.drop('Unknown')['Cases'].sum()
        known_race_deaths = race_df.drop('Unknown')['Deaths'].sum()
        pct_aa_cases = to_percentage(aa_cases, known_race_cases)
        pct_aa_deaths = to_percentage(aa_deaths, known_race_deaths)

        return [self._make_series(
            date=date,
            cases=cases,
            deaths=deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=pct_aa_cases,
            pct_aa_deaths=pct_aa_deaths,
            pct_includes_unknown_race=False,
            pct_includes_hispanic_black=True,
            known_race_cases=known_race_cases,
            known_race_deaths=known_race_deaths
        )]
