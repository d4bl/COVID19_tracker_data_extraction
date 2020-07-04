import covid19_scrapers.utils as utils


def test_as_list():
    assert utils.misc.as_list('') == ['']
    assert utils.misc.as_list(['']) == ['']
