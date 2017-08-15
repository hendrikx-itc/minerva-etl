# -*- coding: utf-8 -*-
import logging

from minerva.storage.trend.aggregate import arith_expr


def setup_module():
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    root_logger.addHandler(stream_handler)


class Context():
    def __init__(self):
        self.trends_meta_by_timestamp = {}
        self.tables_by_timestamp_trend = {}
        self.tables = set()

    def get_trend_column(self, datasource_name, trend_name):
        trend_meta = [
            (trend_name, 1, "{}".format(datasource_name)),
            (trend_name, 2, "{}".format(datasource_name))]

        for trend_name, partition_start, partition_name in trend_meta:
            self.tables.add(partition_name)

            self.trends_meta_by_timestamp.setdefault(
                partition_start, []).append((trend_name, partition_name))

            self.tables_by_timestamp_trend.setdefault(
                partition_start, {})[trend_name] = partition_name

        table_name = "t{}".format(len(self.tables))

        return "{}.\"{}\"".format(table_name, trend_name)


def test_parser_addition():
    formula_str = "3 + 19+2  + 0"

    formula = arith_expr.parseString(formula_str, parseAll=True)[0]

    context = Context()

    r = formula(context)

    assert r == "3 + 19 + 2 + 0"


def test_parser_subtraction():
    formula_str = "42-6 - 2  - 5"

    formula = arith_expr.parseString(formula_str, parseAll=True)[0]

    context = Context()

    r = formula(context)

    assert r == "42 - 6 - 2 - 5"


def test_parser_brackets():
    formula_str = "4 / 7 + (2/-3)"

    formula = arith_expr.parseString(formula_str, parseAll=True)[0]

    context = Context()

    r = formula(context)

    assert r == "4 / 7 + (2 / -3)"


def test_parser_advanced_a():
    formula_str = "SUM(CCR) / SUM(Traffic_full) + (2/-3)"

    formula = arith_expr.parseString(formula_str, parseAll=True)[0]

    context = Context()

    r = formula(context)

    assert r == "SUM(t1.\"CCR\") / SUM(t1.\"Traffic_full\") + (2 / -3)"


def test_parser_advanced_b():
    formula_str = 'SUM(CCR * src1."Traffic-Full") / SUM(src2."Traffic-full")'

    formula = arith_expr.parseString(formula_str, parseAll=True)[0]

    context = Context()

    r = formula(context)

    assert r == "SUM(t1.\"CCR\" * t2.\"Traffic-Full\")" \
                "/ SUM(t3.\"Traffic-full\")"
