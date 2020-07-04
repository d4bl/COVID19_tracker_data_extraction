# Text parsing helpers.
def raw_string_to_int(s):
    """Some parsed strings have additional elements attached to them such
    as `\n` or `,`.  This function filters those elements out and
    casts the string to an int.

    It throws ValueError if the string is empty.

    """
    return int(''.join([c for c in s if c.isnumeric()]))


def maybe_convert(val):
    """Remove certain characters from the input string, then tries
    to convert it to an integer, falling back to a float, falling back
    to the modified string.

    """
    val = val.replace(',', '').replace('%', '').replace('NA', 'nan').strip()
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        return val