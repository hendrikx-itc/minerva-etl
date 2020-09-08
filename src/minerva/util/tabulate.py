# -*- coding: utf-8 -*-
from functools import partial, reduce
from typing import List, Iterator, Iterable

WITH_ACCS = {
    "auto": min,
    "min": min,
    "max": max
}


def join(sep, items):
    it = iter(items)

    first = next(it)

    yield first

    for v in it:
        yield sep
        yield v


def render_rst_table(column_names: List[str], column_align: List[str], column_sizes: List[str], rows: List[Iterable]) -> List[str]:
    """
    ReStructuredText table
    """
    col_count = len(column_names)
    col_sep = " | "
    col_widths = calc_column_widths(column_names, rows, column_sizes)

    header = "| " + render_line(
        col_sep, column_names, col_widths, "^" * col_count) + " |"
    horizontal_sep = "+-" + render_horizontal_sep(
        "-+-", "-", col_widths) + "-+"
    horizontal_sep_header = "+=" + render_horizontal_sep(
        "=+=", "=", col_widths) + "=+"
    body_rows = [
        "| " + render_line(col_sep, row, col_widths, column_align) + " |"
        for row in rows
    ]

    return (
        [horizontal_sep, header, horizontal_sep_header] +
        list(join(horizontal_sep, body_rows)) +
        [horizontal_sep]
    )


def render_table(column_names: List[str], column_align: Iterable[str], column_sizes: List[str], rows: List[Iterable]) -> List[str]:
    """
    Return list of lines of rendered table.

    Example:

    rows = [
        [1, "first line"],
        [2, "second line"]
        [3, "third line"]]

    column_names = ["nr", "text"]
    column_align = "<" * len(column_names) # Align all columns left
    column_sizes = ["max"] * len(column_names) # Size columns to max cell width

    Calling render_table(column_names, column_align, column_sizes, rows)
    results in:

    [
        "nr |        text",
        "---+------------",
        "1  | first line ",
        "2  | second line",
        "3  | third line "
    ]

    To print it to screen, use print("\n".join(lines))
    """
    col_count = len(column_names)
    col_sep = " | "
    col_widths = calc_column_widths(column_names, rows, column_sizes)

    header = render_line(col_sep, column_names, col_widths, "^" * col_count)
    horizontal_sep = render_horizontal_sep("-+-", "-", col_widths)
    body = [
        render_line(col_sep, row, col_widths, column_align)
        for row in rows
    ]

    return [header, horizontal_sep] + body


def calc_column_widths(column_names, rows, column_sizes):
    header_widths = map(len, map(str, column_names))

    widths = [map(len, map(str, row)) for row in rows]

    accumulators = [WITH_ACCS[column_size] for column_size in column_sizes]

    accumulator = partial(reduce_row, accumulators)

    return reduce(accumulator, widths, header_widths)


def reduce_row(accumulators, row1, row2):
    return [
        acc(val1, val2)
        for acc, val1, val2 in zip(accumulators, row1, row2)
    ]


def render_line(col_sep, values, widths, alignments):
    return col_sep.join(
        render_column(value, width, alignment)
        for value, width, alignment in zip(values, widths, alignments)
    )


def render_horizontal_sep(col_sep, hor_sep, widths):
    return col_sep.join(hor_sep * width for width in widths)


def render_column(value, width, alignment):
    result = "{0: {2}{1}}".format(str(value), width, alignment)

    return result[:width]
