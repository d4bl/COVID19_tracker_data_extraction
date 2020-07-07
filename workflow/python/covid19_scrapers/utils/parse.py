def raw_string_to_int(s, error='raise', default=None):
    """Some parsed strings have additional elements attached to them such
    as `\n` or `,`.  This function filters those elements out and
    casts the string to an int.

    Params:
        s: the string to be parsed
        error: either 'raise' or 'return_default'. 'raise' will raise an error on an empty parsed string.
            'return_default' will return the value of `default` on an empty parsed string
        default: if error is set to `return_default,` this value will be returns on an empty parsed string
    """
    if error not in ['raise', 'return_default']:
        raise ValueError(f'raw_string_to_int: error param must be "raise" or "return_default", not "{error}"')
    nums = [c for c in s if c.isnumeric()]
    if nums:
        return int(''.join(nums))
    if error == 'raise':
        raise ValueError(f'Unable to parse "{s}" as int')
    else:
        return default


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
