import logging

import pandas as pd

from covid19_scrapers.utils import get_content_as_file


_logger = logging.getLogger(__name__)


class FipsLookup(object):
    """This extracts and indexes the geographic codes needed for the
    Census API and makes them easier to look up.  The 2018 vintage
    codes are extracted from a spreadsheet, and stored in lookup
    tables by "summary level", the combination of codes needed to
    identify a census geography.

    """
    CODES_URL = 'https://www2.census.gov/programs-surveys/popest/geographies/2018/all-geocodes-v2018.xlsx'

    def __init__(self):
        df = pd.read_excel(get_content_as_file(self.CODES_URL), skiprows=4)
        df = df.rename(columns={
            'Area Name (including legal/statistical area description)': 'Name',
            'State Code (FIPS)': 'STATEFP',
            'County Code (FIPS)': 'COUNTYFP',
            'County Subdivision Code (FIPS)': 'COUSUBFP',
            'Place Code (FIPS)': 'PLACEFP',
            'Consolidtated City Code (FIPS)': 'CONCITFP',  # sic
            'Consolidated City Code (FIPS)': 'CONCITFP',  # in case they fix it
        })

        # Turn codes back into zero-padded strings
        df['STATEFP'] = df['STATEFP'].apply(lambda x: f'00000{x}'[-2:])
        df['COUNTYFP'] = df['COUNTYFP'].apply(lambda x: f'00000{x}'[-3:])
        df['COUSUBFP'] = df['COUSUBFP'].apply(lambda x: f'00000{x}'[-5:])
        df['PLACEFP'] = df['PLACEFP'].apply(lambda x: f'00000{x}'[-5:])
        df['CONCITFP'] = df['CONCITFP'].apply(lambda x: f'00000{x}'[-5:])
        df['Summary Level'] = df['Summary Level'].apply(
            lambda x: f'00000{x}'[-3:])

        # States
        state_df = df[df['Summary Level'] == '040'].reindex(
            columns=['Name', 'STATEFP'], copy=True)
        self.state_df = state_df.set_index('Name').sort_index()

        # Counties
        county_df = df[df['Summary Level'] == '050'].reindex(
            columns=['Name', 'STATEFP', 'COUNTYFP'], copy=True)
        county_df['Name'] = county_df['Name'].str.replace(' County$', '')
        county_df = county_df.drop_duplicates(subset=['STATEFP', 'Name'])
        self.county_df = county_df.set_index(['STATEFP', 'Name']).sort_index()

        # County subdivisions
        cousub_df = df[df['Summary Level'] == '061'].reindex(
            columns=['Name', 'STATEFP', 'COUNTYFP', 'COUSUBFP'], copy=True)
        cousub_df['Name'] = cousub_df['Name'].str.replace(' [a-z ]+$', '')
        cousub_df = cousub_df.drop_duplicates(subset=['STATEFP', 'Name'])
        self.cousub_df = cousub_df.set_index(['STATEFP', 'Name']).sort_index()

        # Places
        place_df = df[df['Summary Level'] == '162'].reindex(
            columns=['Name', 'STATEFP', 'PLACEFP'], copy=True)
        place_df['Name'] = place_df['Name'].str.replace(' [a-z ]+$', '')
        place_df = place_df.drop_duplicates(subset=['STATEFP', 'Name'])
        self.place_df = place_df.set_index(['STATEFP', 'Name']).sort_index()

        # Consolidated cities
        concit_df = df[df['Summary Level'] == '170'].reindex(
            columns=['Name', 'STATEFP', 'CONCITFP'], copy=True)
        concit_df['Name'] = concit_df['Name'].str.replace(' [a-z ]+$', '')
        concit_df = concit_df.drop_duplicates(subset=['STATEFP', 'Name'])
        self.concit_df = concit_df.set_index(['STATEFP', 'Name']).sort_index()

    def lookup_state(self, state):
        """Find the FIPS code for a state.

        Arguments:
          state: An unabbeviated state name, such as "New York".

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for argument.

        """
        return {
            'geo_for': {
                'state': self.state_df.loc[state, 'STATEFP']
            }
        }

    def lookup_county(self, state, county):
        """Find the FIPS code for a county.

        Arguments:
          state: An unabbeviated state name, such as "New York".
          county: The name of a county in that state, with no "County"
            suffix.  E.g., "Queens"

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for and geo_in arguments.

        """
        statefp = self.state_df.loc[state, 'STATEFP']
        return {
            'geo_in': {
                'state': statefp
            },
            'geo_for': {
                'county': self.county_df.loc[(statefp, county), 'COUNTYFP']
            }
        }

    def lookup_place(self, state, place):
        """Find the FIPS code for an inhabited place.

        Arguments:
          state: An unabbeviated state name, such as "New York".
          place: The name of a place in that state, with no "town",
            etc, suffix.  E.g., "New York"

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for and geo_in arguments.

        """
        statefp = self.state_df.loc[state, 'STATEFP']
        return {
            'geo_in': {
                'state': statefp
            },
            'geo_for': {
                'place': self.place_df.loc[
                    (statefp, place), 'PLACEFP'],
            }
        }

    def lookup_county_subdivision(self, state, cousub):
        """Find the FIPS code for a county subdivision.

        Arguments:
          state: An unabbeviated state name, such as "North Dakota".
          cousub: The name of a county subdivision (township) in that
            state, with no "town", etc, suffix.  E.g., "Albertha"

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for and geo_in arguments.

        """
        statefp = self.state_df.loc[state, 'STATEFP']
        row = self.cousub_df.loc[(statefp, cousub)]
        return {
            'geo_in': {
                'state': statefp,
                'county': row['COUNTYFP']
            },
            'geo_for': {
                'county subdivision': row['COUSUBFP']
            }
        }

    def lookup_consolidated_city(self, state, concit):
        """Find the FIPS code for a consolidated city. These are
        the handful of cities in the US that coincide with their
        containing counties.

        Arguments:
          state: An unabbeviated state name, such as "Tennessee".
          concit: The name of a consolidated city in that
            state, with no suffix.  E.g., "Nashville-Davidson"

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for and geo_in arguments.

        """
        statefp = self.state_df.loc[state, 'STATEFP']
        return {
            'geo_in': {
                'state': statefp
            },
            'geo_for': {
                'consolidated city': self.concit_df.loc[
                    (statefp, concit), 'CONCITFP'],
            }
        }

    def lookup_city(self, state, city):
        """Find the FIPS code for a colloquial city. These can be any of
        places, county subdivisions, or consolidated cities. This
        simply attempts to find city as a place, county subdivision,
        or consolidated city, in that order.  This interface is useful
        since the distinction is rarely one people make when trying to
        find stats on a place.  This is "lossy" in that there are
        examples where there is a core incorporated city (place)
        surrounded by a township (county subdivision), and this will
        not let you distinguish them.  (Example: City of Poughkeepsie
        and Town of Poughkeepsie in NY.)

        Arguments:
          state: An unabbeviated state name, such as "California".
          city: The name of a city in that
            state, with no suffix.  E.g., "Sacramento"

        Returns a dict containing arguments suitable for
        CensusApi.get_acs5_data's geo_for and geo_in arguments.

        """
        # We will look for places first.
        try:
            return self.lookup_place(state, city)
        except KeyError:
            pass

        # Next county subdivisions
        try:
            return self.lookup_county_subdivision(state, city)
        except KeyError:
            pass

        # Consolidated cities are uncommon, so we save them for last
        try:
            return self.lookup_consolidated_city(state, city)
        except KeyError:
            pass
        raise KeyError(f'Unable to find code for city: {city}')
