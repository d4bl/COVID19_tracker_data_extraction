import json

import pydash

from covid19_scrapers.utils.tableau.tableau_util import extract_json_from_blob


def get_response_handler(blob):
    """From what I observed so far, Tableau data can be returned with 3 different types of formats

    Each handler is responsible for parsing out a specific format.
    Each handler has 3 methods:
        - get_dashboard_sections
        - get_values_lookup
        - get_zone_lookup
    which the TableauParser uses to obtain the needed data to continue extracting data.
    """
    json1, json2 = extract_json_from_blob(blob)
    if json1 and json2:  # this is either v1 or v2
        if pydash.get(json2, 'secondaryInfo.presModelMap.vizData'):
            return TableauV1Handler(json1, json2)
        else:
            return TableauV2Handler(json1, json2)
    else:  # VSQL info
        return TableauVSQLHandler(json.loads(blob))


class TableauResponseHandler(object):
    def __init__(self, *args, **kwargs):
        pass

    def get_dashboard_sections(self):
        raise NotImplementedError()

    def get_values_lookup(self):
        raise NotImplementedError()

    def get_zone_lookup(self):
        return {}


class TableauV1Handler(TableauResponseHandler):
    def __init__(self, viz_data, json_data):
        self.viz_data = viz_data
        self.json_data = json_data

    def get_dashboard_sections(self):
        return pydash.get(self.json_data, 'secondaryInfo.presModelMap.vizData.presModelHolder.'
                                          'genPresModelMapPresModel.presModelMap')

    def get_values_lookup(self):
        lookup = pydash.get(self.json_data, 'secondaryInfo.presModelMap.dataDictionary.presModelHolder.'
                                            'genDataDictionaryPresModel.dataSegments.0.dataColumns')
        return {v.get('dataType'): v.get('dataValues') for v in lookup}


class TableauV2Handler(TableauResponseHandler):
    SECTION_KEY = 'worldUpdate.applicationPresModel.workbookPresModel.dashboardPresModel.zones'

    def __init__(self, viz_data, json_data):
        self.viz_data = viz_data
        self.json_data = json_data
        self.section_with_data = pydash.head([v for _, v in pydash.get(self.viz_data, self.SECTION_KEY, {}).items()
                                              if pydash.get(v, 'presModelHolder.flipboard')])

    def get_dashboard_sections(self):
        return pydash.get(
            self.section_with_data, 'presModelHolder.flipboard.storyPoints.1.dashboardPresModel.zones')

    def get_values_lookup(self):
        lookup = pydash.get(self.json_data, 'secondaryInfo.presModelMap.dataDictionary.presModelHolder.'
                                            'genDataDictionaryPresModel.dataSegments.0.dataColumns')
        return {v.get('dataType'): v.get('dataValues') for v in lookup}

    def get_zone_lookup(self):
        return {pydash.get(v, 'zoneCommon.name'): k
                for k, v in pydash.get(self.section_with_data, 'presModelHolder.flipboard.storyPoints.'
                                                               '1.dashboardPresModel.zones', {}).items()}


class TableauVSQLHandler(TableauResponseHandler):
    SECTION_KEY = 'vqlCmdResponse.layoutStatus.applicationPresModel.workbookPresModel.dashboardPresModel.zones'

    def __init__(self, vql_response):
        self.vql_response = vql_response
        self.section_with_data = pydash.head([v for _, v in pydash.get(self.vql_response, self.SECTION_KEY, {}).items()
                                              if pydash.get(v, 'presModelHolder.flipboard')])

    def get_dashboard_sections(self):
        return pydash.get(self.section_with_data, 'presModelHolder.flipboard.storyPoints.2.dashboardPresModel.zones')

    def get_values_lookup(self):
        lookup = pydash.get(self.vql_response, 'vqlCmdResponse.layoutStatus.applicationPresModel.'
                                               'dataDictionary.dataSegments.0.dataColumns', {})
        return {v.get('dataType'): v.get('dataValues') for v in lookup}

    def get_zone_lookup(self):
        return {pydash.get(v, 'zoneCommon.name'): k
                for k, v in pydash.get(self.section_with_data, 'presModelHolder.flipboard.storyPoints.'
                                                               '2.dashboardPresModel.zones', {}).items()}
