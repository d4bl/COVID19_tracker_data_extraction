import json
import re

import pydash


def extract_json_from_response(response):
    # There is some id attached to the beginning of each json response,
    # so look for that number with a curly brace and replace it with a curly brace.
    expr = r'(\d+;)(\{)'
    json_str_blobs = re.sub(expr, '{', response.body.decode('utf8'))
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


def extract_data_from_key(json_data, key):
    lookup = pydash.get(json_data, 'secondaryInfo.presModelMap.dataDictionary.presModelHolder.'
                        'genDataDictionaryPresModel.dataSegments.0.dataColumns')
    values_lookup = {v.get('dataType'): v.get('dataValues') for v in lookup}
    dashboard_sections = pydash.get(json_data, 'secondaryInfo.presModelMap.vizData.presModelHolder.'
                                    'genPresModelMapPresModel.presModelMap')
    metadata = pydash.get(dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel.'
                          'paneColumnsData.vizDataColumns')
    aliased_values = pydash.get(dashboard_sections, f'{key}.presModelHolder.genVizDataPresModel.paneColumnsData.'
                                'paneColumnsList.0.vizPaneColumns')
    data = {}
    for meta, aliased_value in zip(metadata, aliased_values):
        if 'fieldCaption' not in meta:
            continue
        alias_indices = aliased_value.get('aliasIndices')
        is_measure = meta['fieldRole'] == 'measure'
        data[meta['fieldCaption']] = try_to_unalias(values_lookup, alias_indices, is_measure)
    return data


def try_to_unalias(values_lookup, alias_indices, is_measure):
    for data_type, lookup in values_lookup.items():
        try:
            if is_measure:
                alias_indices = [abs(idx) - 1 if idx < 0 else abs(idx) for idx in alias_indices]
            return [values_lookup[data_type][abs(idx)] for idx in alias_indices]
        except IndexError:
            continue
    raise Exception('Could not unalias values.')
