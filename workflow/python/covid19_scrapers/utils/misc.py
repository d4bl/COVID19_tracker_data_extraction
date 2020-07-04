def as_list(arg):
    """If arg is a list, return it.  Otherwise, return a list containing
    it.

    This is to make it easier to consume tabula.read_pdf output, which
    is either a table or a list of tables.
    """
    if isinstance(arg, list):
        return arg
    return [arg]


def to_percentage(numerator, denominator, round_num_digits=2):
    """Copies of this code are used in almost all the scrapers to
    calculate Black/AA death and case percentages.

    """
    return round((numerator / denominator) * 100, round_num_digits)
