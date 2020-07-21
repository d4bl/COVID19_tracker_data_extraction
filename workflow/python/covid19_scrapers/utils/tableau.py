import json
import re

import pydash


def find_tableau_request(request):
    return 'bootstrapSession' in request.path


class TableauParserException(Exception):
    pass


class TableauParser(object):
    """Tableau returns back data to populate a dashboard with a response that contains 2 json blobs

    This data can be parsed out and used for obtaining data from the dashboard.

    This class takes the body of the response (`blob`) and parses it. Afterward,
    calls to `extract_data_from_key` can be used to extract needed data.
    """

    def __init__(self, blob=None, *, request=None):
        if request:
            blob = request.response.body.decode('utf8')
        self.blob = blob
        self.viz_data, self.json_data = self._extract_json_from_blob(self.blob)
        self.v2_dashboard_lookup = None
        self.values_lookup = self._values_lookup()
        self.dashboard_sections = self._dashboard_sections()

    def _extract_json_from_blob(self, blob):
        """Given a blob returned from a specific Tableau request, this function aims to
        return back the various json blobs back as a list.

        There is some id attached to the beginning of each json response,
        so look for that number with a curly brace and replace it with a curly brace.

        Then parse out the json data.
        """
        expr = r'(\d+;)(\{)'
        json_str_blobs = re.sub(expr, '{', blob)
        decoder = json.JSONDecoder()
        pos = 0
        json_data = []
        while True:
            try:
                obj, pos = decoder.raw_decode(json_str_blobs, pos)
                json_data.append(obj)
            except json.JSONDecodeError:
                break
        assert len(json_data) == 2, 'Response should have 2 JSON blobs, incorrect number of blobs extracted'
        return json_data

    def _dashboard_sections(self):
        db_section = self._v1_dashboard_section() or self._v2_dashboard_section()
        if not db_section:
            raise TableauParserException('No dashboard sections could be found with parsed data')
        return db_section

    def _values_lookup(self):
        lookup = pydash.get(self.json_data, 'secondaryInfo.presModelMap.dataDictionary.presModelHolder.'
                            'genDataDictionaryPresModel.dataSegments.0.dataColumns')
        values_lookup = {v.get('dataType'): v.get('dataValues') for v in lookup}
        if not values_lookup:
            raise TableauParserException('No lookup values found for the request')
        return values_lookup

    def list_keys(self):
        """A debugging function used for checking which keys are extractable via
        the `extract_data_from_keys` method.
        """
        if self.v2_dashboard_lookup:
            return list(self.v2_dashboard_lookup.keys())
        else:
            return list(self.dashboard_sections.keys())

    def update_with_vql_command_request(self, request):
        vql_response = json.loads(request.response.body.decode('utf8'))
        section_with_data = pydash.head([v for _, v in vql_response['vqlCmdResponse']['layoutStatus']['applicationPresModel']['workbookPresModel']['dashboardPresModel']['zones'].items() if pydash.get(v, 'presModelHolder.flipboard')])
        self.v2_dashboard_lookup = {pydash.get(x, 'zoneCommon.name'): k for k, x in section_with_data['presModelHolder']['flipboard']['storyPoints']['2']['dashboardPresModel']['zones'].items()}
        self.dashboard_sections = section_with_data['presModelHolder']['flipboard']['storyPoints']['2']['dashboardPresModel']['zones']
        if not self.dashboard_sections:
            raise TableauParserException('No dashboard sections could be found with parsed data')
        lookup = vql_response['vqlCmdResponse']['layoutStatus']['applicationPresModel']['dataDictionary']['dataSegments']['0']['dataColumns']
        self.values_lookup = {v.get('dataType'): v.get('dataValues') for v in lookup}
        if not self.values_lookup:
            raise TableauParserException('No lookup values found for the request')

    def _v1_dashboard_section(self):
        return pydash.get(self.json_data, 'secondaryInfo.presModelMap.vizData.presModelHolder.'
                                          'genPresModelMapPresModel.presModelMap')

    def _v2_dashboard_section(self):
        section_with_data = pydash.head([v for _, v in self.viz_data['worldUpdate']['applicationPresModel']['workbookPresModel']['dashboardPresModel']['zones'].items() if pydash.get(v, 'presModelHolder.flipboard')])
        self.v2_dashboard_lookup = {pydash.get(x, 'zoneCommon.name'): k for k, x in section_with_data['presModelHolder']['flipboard']['storyPoints']['1']['dashboardPresModel']['zones'].items()}
        return section_with_data['presModelHolder']['flipboard']['storyPoints']['1']['dashboardPresModel']['zones']

    def _v1_metadata(self, key):
        return pydash.get(self.dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel'
                          '.paneColumnsData.vizDataColumns')

    def _v2_metadata(self, key):
        zone = self.v2_dashboard_lookup[key]
        return pydash.get(self.dashboard_sections, f'{zone}.presModelHolder.visual.vizData.'
                          'paneColumnsData.vizDataColumns')

    def get_metadata(self, key):
        return self._v1_metadata(key) or self._v2_metadata(key)

    def _v1_aliased_values(self, key):
        return pydash.get(self.dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel.paneColumnsData.'
                          'paneColumnsList.0.vizPaneColumns')

    def _v2_aliased_values(self, key):
        zone = self.v2_dashboard_lookup[key]
        return pydash.get(self.dashboard_sections, f'{zone}.presModelHolder.visual.vizData.paneColumnsData.'
                          'paneColumnsList.0.vizPaneColumns')

    def get_aliased_values(self, key):
        return self._v1_aliased_values(key) or self._v2_aliased_values(key)

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
