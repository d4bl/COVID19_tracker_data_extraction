import pydash

from covid19_scrapers.utils.tableau.response_handler import get_response_handler


class TableauParserException(Exception):
    pass


class TableauParser(object):
    """Tableau returns back data to populate a dashboard with a response that contains 2 json blobs

    This data can be parsed out and used for obtaining data from the dashboard.

    This class takes the body of the response (`blob`) or a request and parses it. Afterward,
    calls to `extract_data_from_key` can be used to extract needed data.
    """

    def __init__(self, blob=None, *, request=None):
        if request:
            blob = request.response.body.decode('utf8')
        self.blob = blob
        response_handler = get_response_handler(self.blob)
        self.values_lookup = response_handler.get_values_lookup()
        self.dashboard_sections = response_handler.get_dashboard_sections()
        self.zone_lookup = response_handler.get_zone_lookup()

    def list_keys(self):
        """A debugging function used for checking which keys are extractable via
        the `extract_data_from_keys` method.
        """
        if self.zone_lookup:
            return list(self.zone_lookup.keys())
        else:
            return list(self.dashboard_sections.keys())

    def _no_zone_meta_lookup(self, key):
        return pydash.get(self.dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel'
                          '.paneColumnsData.vizDataColumns')

    def _zone_meta_lookup(self, key):
        zone = self.zone_lookup[key]
        return pydash.get(self.dashboard_sections, f'{zone}.presModelHolder.visual.vizData.'
                          'paneColumnsData.vizDataColumns')

    def get_metadata(self, key):
        return self._no_zone_meta_lookup(key) or self._zone_meta_lookup(key)

    def _no_zone_aliased_values(self, key):
        return pydash.get(self.dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel.paneColumnsData.'
                          'paneColumnsList.0.vizPaneColumns')

    def _zone_aliased_values(self, key):
        zone = self.zone_lookup[key]
        return pydash.get(self.dashboard_sections, f'{zone}.presModelHolder.visual.vizData.paneColumnsData.'
                          'paneColumnsList.0.vizPaneColumns')

    def get_aliased_values(self, key):
        return self._no_zone_aliased_values(key) or self._zone_aliased_values(key)

    def extract_data_from_key(self, key):
        """As the json blobs are extracted from Tableau, the second piece of information from the extracted data
        contains data about each dashboard that is shown.

        Each dashboard is partioned off by a specific key. This function reads the json data and makes
        the data from the key available in a dictionary format.
        """
        assert key in self.list_keys(), 'Key not in dashboard_sections. Valid keys for this data: %s' % self.list_keys()
        metadata = self.get_metadata(key)
        aliased_values = self.get_aliased_values(key)
        data = {}
        for meta, aliased_value in zip(metadata, aliased_values):
            if 'fieldCaption' not in meta:
                continue
            alias_indices = aliased_value.get('aliasIndices')
            data[meta['fieldCaption']] = self._try_to_unalias(alias_indices, meta)
        return data

    def _try_to_unalias(self, alias_indices, meta):
        try:
            data_type = meta.get('dataType')
            return self._unalias_by_data_type(alias_indices, meta, data_type)
        except (IndexError, KeyError):
            return self._try_hard_to_unalias(alias_indices, meta)

    def _unalias_by_data_type(self, alias_indices, meta, data_type):
        if meta['fieldRole'] == 'measure' or meta['dataType'] == 'date':
            alias_indices = [abs(idx) - 1 if idx < 0 else idx for idx in alias_indices]
        if meta['dataType'] == 'date':
            data_type = 'cstring'
        return [self.values_lookup[data_type][abs(idx)] for idx in alias_indices]

    def _try_hard_to_unalias(self, alias_indices, meta):
        for data_type, lookup in self.values_lookup.items():
            try:
                if meta['fieldRole'] == 'measure':
                    alias_indices = [abs(idx) - 1 if idx < 0 else idx for idx in alias_indices]
                return [self.values_lookup[data_type][abs(idx)] for idx in alias_indices]
            except (IndexError, KeyError):
                continue
        raise TableauParserException('Could not unalias values.')
