from functools import partial


def compile_union_view_query(ident, sources):
    union = compile_union(sources)

    return "CREATE VIEW {} AS {}".format(render_ident(ident), union)


def compile_union(sources):
    """
    :param sources: a list of tuples (table, columns)
    """
    return " UNION ".join(map(compile_select, sources))


def compile_select(source):
    """
    :param source: a tuple (table, columns)
    """
    table, columns = source

    columns_part = ", ".join(map(render_column, columns))

    return "SELECT {} FROM {}".format(columns_part, render_ident(table))


def render_column(column):
    if isinstance(column, tuple):
        name, alias = column

        return "{} as {}".format(quote(name), quote(alias))
    elif isinstance(column, str):
        return quote(column)


def render_ident(ident):
    if hasattr(ident, "__iter__"):
        parts = ident
    elif isinstance(ident, str):
        parts = ident,

    return ".".join(map(quote, parts))


enclose = partial(str.format, "{0[0]}{1}{0[1]}")


quote = partial(enclose, '""')
