import logging

_logger = logging.getLogger(__name__)


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


def slice_dataframe(df, start_val, end_val):
    """Return a sub-DataFrame whose index values are contained in the
       closed interval [start_val, end_val].

    """
    # Make sure the DF is sorted descending.
    df = df.sort_index(ascending=False)
    end_ix = df.index.get_slice_bound(end_val, 'left', 'ix')
    if not start_val:
        if end_ix == 0:
            start_val = df.index.max()
        else:
            start_val = df.iloc[end_ix].name
    start_ix = df.index.get_slice_bound(start_val, 'right', 'ix')
    if start_ix == end_ix:
        if start_ix == 0:
            # Vals are both to the left of the index values.
            end_ix += 1
        else:
            # Vals are both to the right of the index values.
            start_ix -= 1
    return df.iloc[end_ix:start_ix]
