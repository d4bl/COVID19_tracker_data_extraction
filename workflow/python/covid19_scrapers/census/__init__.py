__all__ = ['CensusApi', 'FipsLookup', 'get_aa_pop_stats']

from covid19_scrapers.census.census_api import CensusApi
from covid19_scrapers.census.fips_lookup import FipsLookup
from covid19_scrapers.utils.misc import to_percentage


def get_aa_pop_stats(census_api, state, *, county=None, city=None):
    """Using the provided CensusApi instance, this routine fetches
    Black/AA population, and return that, the total population, and
    the percentage Black/AA.

    Arguments:
      census_api: the CensusApi instance.
      state: the state for which to fetch data.
      county: if present the county in `state` for which to fetch data.
      city: if present the city in `state` for which to fetch data.
        If county and city are both present, city will be retrieved.

    Returns a 3-tuple of Black/AA population, total population, and
    percentage Black/AA.

    """
    df = census_api.get_pop_by_race(state, county=county, city=city)
    assert df.shape[0] == 1, f'Unexpected row count from Census API: {df.shape[0]}'
    row = df.iloc[0]
    total_pop = row['RACE_Estimate!!Total']
    aa_pop = row['RACE_Estimate!!Total!!Black or African American alone']
    return (int(aa_pop), int(total_pop),
            to_percentage(int(aa_pop), int(total_pop)))
