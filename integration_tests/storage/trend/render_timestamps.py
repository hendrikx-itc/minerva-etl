import datetime
from functools import partial
from functools import reduce
from minerva.directory.basetypes import DataSource
from minerva.storage.trend.tables import make_table_name

WITH_ACCS = {
    "auto": min,
    "min": min,
    "max": max
}

GP_QTR = 900
GP_HR = 3600
GP_DAY = 86400


def create_datasource(timezone):
    return DataSource(
        1, name="DummySource",
        description="Dummy data source",
        timezone=timezone)


ENTITYTYPE_NAME = "DummyType"
DATASOURCE = create_datasource("Europe/Amsterdam")


def main():
    timestamp = DATASOURCE.tzinfo.localize(
        datetime.datetime(2008, 12, 3, 1, 0, 0))

    timestamps = [
        timestamp + i * datetime.timedelta(
            0, GP_HR) for i in range(7 * 24)]

    column_names = ["timestamp", "table_name"]
    alignments = ["<", "<"]
    widths = ["max", "max"]

    table_names = map(partial(
        make_table_name, DATASOURCE, GP_HR, ENTITYTYPE_NAME), timestamps)

    rows = zip(timestamps, table_names)

    table = render_table(column_names, alignments, widths, rows)

    for line in table:
        print(line)


def render_table(column_names, column_align, column_sizes, rows):
    col_count = len(column_names)
    col_sep = " | "
    col_widths = calc_column_widths(column_names, rows, column_sizes)

    header = render_line(col_sep, column_names, col_widths, ">" * col_count)
    horizontal_sep = render_horizontal_sep("-+-", "-", col_widths)
    body = [render_line(
        col_sep, row, col_widths, column_align) for row in rows]

    return [header, horizontal_sep] + body


def calc_column_widths(column_names, rows, column_sizes):
    header_widths = map(len, map(str, column_names))

    widths = [map(len, map(str, row)) for row in rows]

    accumulators = [WITH_ACCS[column_size] for column_size in column_sizes]

    accumulator = partial(reduce_row, accumulators)

    return reduce(accumulator, widths, header_widths)


def reduce_row(accumulators, row1, row2):
    return [acc(val1, val2) for acc, val1, val2 in zip(
        accumulators, row1, row2)]


def render_line(col_sep, values, widths, alignments):
    return col_sep.join(render_column(
        value, width, alignment) for value, width, alignment in zip(
            values, widths, alignments))


def render_horizontal_sep(col_sep, hor_sep, widths):
    return col_sep.join(hor_sep * width for width in widths)


def render_column(value, width, alignment):
    result = "{0: {2}{1}}".format(str(value), width, alignment)

    return result[:width]


if __name__ == "__main__":
    main()
