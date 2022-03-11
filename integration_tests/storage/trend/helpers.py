"""Helper functions unrelated to other functionality."""
from minerva.util import head, tail
from minerva.util.tabulate import render_table


def render_source(source):
    """
    Render a data 'source' in the form of a table-like object.

    Example:
    [
        ('column_1', 'column_2', 'column_3', ...),
        (1, 2, 3,...),
        (4, 5, 6,...),
        ...
    ]
    """
    column_names = head(source)
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = tail(source)

    return render_table(column_names, column_align, column_sizes, rows)
