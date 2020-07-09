import json
import re

import pydash


def extract_json_from_blob(blob):
    """Given a blob returned from a specific Tableau request, this function aims to
    return back the various json blobs back as a list.
    """

    # There is some id attached to the beginning of each json response,
    # so look for that number with a curly brace and replace it with a curly brace.
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


def extract_data_from_key(json_data, key):
    """As the json blobs are extracted from Tableau, the second piece of information from the extracted data
    contains data about each dashboard that is shown.

    Each dashboard is partioned off by a specific key. This function reads the json data and makes
    the data from the key available in a dictionary format.
    """
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
        data[meta['fieldCaption']] = _try_to_unalias(values_lookup, alias_indices, meta)
    return data


def _try_to_unalias(values_lookup, alias_indices, meta):
    try:
        data_type = meta.get('dataType')
        return _unalias_by_data_type(values_lookup, alias_indices, meta, data_type)
    except (IndexError, KeyError):
        return _try_hard_to_unalias(values_lookup, alias_indices, meta)


def _unalias_by_data_type(values_lookup, alias_indices, meta, data_type):
    if meta['fieldRole'] == 'measure' or meta['dataType'] == 'date':
        alias_indices = [abs(idx) - 1 if idx < 0 else idx for idx in alias_indices]
    if meta['dataType'] == 'date':
        data_type = 'cstring'
    return [values_lookup[data_type][abs(idx)] for idx in alias_indices]


def _try_hard_to_unalias(values_lookup, alias_indices, meta):
    for data_type, lookup in values_lookup.items():
        try:
            if meta['fieldRole'] == 'measure':
                alias_indices = [abs(idx) - 1 if idx < 0 else idx for idx in alias_indices]
            return [values_lookup[data_type][abs(idx)] for idx in alias_indices]
        except (IndexError, KeyError):
            continue
    raise Exception('Could not unalias values.')
