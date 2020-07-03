# Retrieve Census demographic information
import logging

import pandas as pd

from covid19_scrapers.census.fips_lookup import FipsLookup
from covid19_scrapers.utils import get_content_as_file, get_json


_logger = logging.getLogger(__name__)


class CensusApi(object):
    """This is a wrapper around the Census API that is primarily intended
    to retrieve population by race.

    For more information on the Census API, see
    https://github.com/nkrishnaswami/census-api-demo/blob/master/Census%20Data%20API%20Demo.ipynb

    The Census API limits users without an API key to 500 requests per
    IP address per day.  If you need more, you can get an API key at
    https://api.census.gov/data/key_signup.html

    Arguments:
      key: your Census API key

    """
    def __init__(self, api_key):
        self.api_key = api_key
        self.fips = FipsLookup()

    @staticmethod
    def _make_get_param(fields, groups):
        """Formats fields and groups as required for the query `get`
        parameter.

        Arguments:
          fields: dict of fields with keys of Census fields and values
            of column names to use for them in the resulting Dataframe.
          groups: list of variable group IDs to format as
            `group({groupId})`.

        Returns a string suitable for using as a query parameter
        value.

        """
        all_fields = list(fields.keys())
        all_fields.extend([f'group({group})' for group in groups])
        return ','.join(all_fields)

    @staticmethod
    def _get_group_names(url, group):
        """Since requesting a variable group returns many variables, it is not
        convenient to pass in column names to use for each of them.
        Instead, we can use the group definition (retrieved via the
        Census API discovery interface) to construct descriptive (if
        verbose) column names for each variable.

        Arguments:
          url: the API dataset endpoint to query.
          group: a variable group ID.

        Returns a dict mapping variable IDs to descriptive column
        names.

        """
        resp = get_json(url + f'/groups/{group}.json')
        for val in resp['variables'].values():
            if val.get('concept'):
                concept = val['concept']
        return {
            key: f'{concept}_{value["label"]}'
            for key, value in resp['variables'].items()
        }

    def get_acs5_data(self, fields, groups, vintage, geo_for, geo_in=None):
        """Retrieve ACS 5-year fields.

        fields: a dict mapping ACS variable names to column names.
        groups: a list of ACS group IDs.
        vintage: the year for which to request data.
        geo_for: a dict containing geographies for which to request data.
        geo_in: a dict containing geographics in which to constrain data.

        Returns a DataFrame with the columns renamed to human-friendly
        names.

        """
        url = f'https://api.census.gov/data/{vintage}/acs/acs5'
        params = {
            'get': CensusApi._make_get_param(fields, groups),
            'for': ' '.join(f'{k}:{v}' for k, v in geo_for.items()),
        }
        if geo_in:
            params['in'] = ' '.join(f'{k}:{v}' for k, v in geo_in.items())
        if self.api_key:
            params['key'] = self.api_key
        else:
            _logger.warn('Calling Census API without a key: '
                         'Be careful of hitting the rate limit.')
        results = get_content_as_file(url, params=params)

        # Get group field to name mappings
        fields = dict(fields)  # Make a copy.
        for group in groups:
            fields.update(CensusApi._get_group_names(url, group))

        df = pd.read_json(results)
        df.columns = df.iloc[0]
        df.columns = [fields.get(column, column) for column in df.columns]
        df = df.iloc[1:]
        return df

    def get_pop_by_race(self, state, *, county=None, city=None, vintage=2018):
        """Retrieve ACS 5-year population for states, counties, or cities.

        Arguments:
          state: the state for/in which to request data.
          county: if present, the county for which to request data.
          city: if present, the city for which to request data. This
            supersedes county, if both are provided.
          vintage: the dataset release year.

        Returns a DataFrame containing non-null values for group
        B02001 (Race).

        """
        if city:
            geo = self.fips.lookup_city(state, city)
        elif county:
            geo = self.fips.lookup_county(state, county)
        else:
            geo = self.fips.lookup_state(state)
        return self.get_acs5_data({'NAME': 'Name'}, ['B02001'],
                                  vintage, **geo).dropna(axis=1)
