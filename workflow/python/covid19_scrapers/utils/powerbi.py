import json

import pandas as pd
import pydash

from covid19_scrapers.utils.misc import as_list


def filter_requests(*, entity=None, selects=None):
    """Some PowerBI response contains information which can be used to obtain data.

    These responses are accompanied by a PowerBI query which contains a From Entity and Selects in the request body.

    In the WebdriverSteps find_by, a way to filter for requests for the needed response
    is by looking in the request body and checking what entity and/or selects the request is querying for.

    to use this method in that fashion:
        WebdriverSteps()
        ...
        .find_request(key=<key>, find_by=filter_requests(entity=<entity>))
        or
        .find_request(key=<key>, find_by=filter_requests(selects=[<selectname1>, <selectname2>]))
    """
    def filter(request):
        body_json = _parse_powerbi_response(request)
        if not body_json:
            return False
        return all([
            _search_by_entity(entity, body_json),
            _serarch_by_selects(selects, body_json)
        ])
    return filter


def _parse_powerbi_response(request):
    if 'querydata' not in request.path:
        return None
    if not request.body:
        return None
    request_body = request.body.decode('utf8').lstrip()
    if not request_body:
        return None
    body_json = json.loads(request_body)
    if not isinstance(body_json, dict):
        return None
    return body_json


def _search_by_entity(entity, body_json):
    if not entity:
        return True
    queries = body_json.get('queries', [])
    query_entities = pydash.flat_map(queries, lambda q: pydash.get(q, 'Query.Commands.0.SemanticQueryDataShapeCommand.Query.From'))
    entity_names = [pydash.get(qe, 'Entity') for qe in query_entities]
    return entity in entity_names


def _serarch_by_selects(selects, body_json):
    if not selects:
        return True
    queries = body_json.get('queries', [])
    resp_selects = pydash.flat_map(queries, lambda q: pydash.get(q, 'Query.Commands.0.SemanticQueryDataShapeCommand.Query.Select'))
    return len(set(as_list(selects)) - {s.get('Name') for s in resp_selects}) == 0


class PowerBIParserException(Exception):
    pass


class PowerBIParser(object):
    """This class makes it so parsing out responses from PowerBI is easier

    This class takes an input of a seleniumwire Request (usually obtained via the WebdriverResults)

    A PowerBI response, is usually composed of 2 parts:
        1. A query select (the client essentially writes a PowerBI query to query for the needed data)
        2. the queried data

    This parser parses out the names of the selects and pairs them with the data.
    To obtain the data that is needed, use the `get_data_by_key` function.

    get_data_by_key takes a substring of a key, and when there is a match on the substring,
    it returns the data associated with the match.

    i.e. if the parsed_data looks like:
        [{'keys': ['ABC', 'DEF'], data: ['really cool data!']},
         {'keys': ['123', '456'], data: ['kinda cool data!']}]

    the way the desired data can be obtained by:
        parser.get_data_by_key(key='AB')

    this would return ['really cool data']
    """

    def __init__(self, request):
        self._dataframes = self._parse_data_from_request(request)

    def _parse_data_from_request(self, request):
        json = self._get_json_from_request(request)
        return self._build_dataframe(json)

    def _get_json_from_request(self, request):
        json_data = json.loads(request.response.body.decode('utf8'))
        return json_data['results']

    def _build_dataframe(self, results):
        dataframes = {}
        for result in results:
            column_names = self._get_column_names(result)
            data_rows = self._get_data(result)
            value_labels = self._get_value_labels(result)
            df = pd.DataFrame(**{'columns': column_names, 'data': data_rows})
            if value_labels:
                df['Label'] = value_labels
            dataframes[' '.join(column_names)] = df
        return dataframes

    def _get_selects(self, result):
        return pydash.get(result, 'result.data.descriptor.Select', [])

    def _get_column_names(self, result):
        selects = self._get_selects(result)
        return [select.get('Name') for select in selects if 'Name' in select]

    def _get_data(self, result):
        selects = self._get_selects(result)
        select_values = [select.get('Value') for select in selects]
        data_rows = pydash.get(result, 'result.data.dsr.DS.0.PH.0.DM0')
        return [self._get_values(select_values, dr) for dr in data_rows]

    def _get_values(self, select_value, data_row):
        row = []
        for sv in select_value:
            if sv and sv in data_row:
                row.append(data_row[sv])
            else:
<<<<<<< HEAD
                return as_list(data_row.get('C', [])) + as_list(data_row.get('R', []))
=======
                return data_row.get('C', [])
>>>>>>> 9d519b1... Update PowerBI parser; update SF scraper
        return row

    def _get_value_labels(self, result):
        return pydash.get(result, 'result.data.dsr.DS.0.ValueDicts.D0')

    def list_keys(self):
        return list(self._dataframes.keys())

    def get_dataframe_by_key(self, key):
        for key_string, df in self._dataframes.items():
            if key in key_string:
                return df
        raise PowerBIParserException('Key not found')
