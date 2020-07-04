import pytest

import covid19_scrapers.utils.parse as parse


def test_raw_string_to_int():
    assert 1 == parse.raw_string_to_int('1')
    assert 1 == parse.raw_string_to_int('1a')
    assert 1000 == parse.raw_string_to_int('1,000')
    assert 100 == parse.raw_string_to_int('100%')
    with pytest.raises(ValueError):
        parse.raw_string_to_int('')
    with pytest.raises(ValueError):
        parse.raw_string_to_int('aaaa')
