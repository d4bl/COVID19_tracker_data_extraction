import json

import pydash


def find_request_query(entity):
    """Some PowerBI response contains information which can be used to obtain data.

    These responses are accompanied by an `Entity` in the request body.

    In the WebdriverSteps find_by, a way to filter for requests for the needed response
    is by looking in the request body and checking what entity the request is querying for.

    to use this method in that fashion:
        WebdriverSteps()
        ...
        .find_request(key=<key>, find_by=find_request_query(entity=<entity>))
    """
    def _find_request_query(request):
        if not request.body:
            return False
        request_body = request.body.decode('utf8').lstrip()
        if not request_body:
            return False
        body_json = json.loads(request_body)
        if not isinstance(body_json, dict):
            return False
        queries = body_json.get('queries', [])
        query_entities = [pydash.get(q, 'Query.Commands.0.SemanticQueryDataShapeCommand.Query.From.0.Entity')
                          for q in queries]
        return entity in query_entities
    return _find_request_query


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
        self.parsed_data = self._parse_data_from_request(request)

    def _parse_data_from_request(self, request):
        json = self._get_json_from_request(request)
        return self._partition_rows(json)

    def _get_json_from_request(self, request):
        json_data = json.loads(request.response.body.decode('utf8'))
        return json_data['results']

    def _partition_rows(self, results):
        return [{'keys': self._get_keys(r), 'data': self._get_data(r)} for r in results]

    def _get_keys(self, result):
        selects = pydash.get(result, 'result.data.descriptor.Select', [])
        return [select.get('Name') for select in selects if 'Name' in select]

    def _get_data(self, json_data):
        return pydash.get(json_data, 'result.data.dsr.DS.0.PH.0.DM0')

    def get_data_by_key(self, key):
        result = pydash.find(self.parsed_data, lambda data: any(key in k for k in data['keys']))
        return result['data']
