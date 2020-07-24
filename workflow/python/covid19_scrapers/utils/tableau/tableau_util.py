import json
import re


def extract_json_from_blob(blob):
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
    if len(json_data) != 2:
        return ({}, {})
    return json_data
