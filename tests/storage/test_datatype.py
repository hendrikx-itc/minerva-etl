# -*- coding: utf-8 -*-
"""
Unit tests for the core.datatype module
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import decimal
from datetime import datetime

from nose.tools import eq_, assert_raises

from minerva.storage import datatype


def test_matches_string():
    assert datatype.DataTypeText.deduce_parser_config(
        "Some string that\nshould be\naccepted."
    )


def test_parse_string():
    value = "Some string that\nshould be\naccepted."

    assert datatype.DataTypeText.string_parser(
        datatype.DataTypeText.string_parser_config({})
    )(value) == value


def test_matches_bigint():
    min_bigint = -pow(2, 63)
    max_bigint = pow(2, 63) - 1

    assert datatype.DataTypeBigint.deduce_parser_config("10")
    assert datatype.DataTypeBigint.deduce_parser_config("-10")
    assert datatype.DataTypeBigint.deduce_parser_config(str(max_bigint))
    assert datatype.DataTypeBigint.deduce_parser_config(str(min_bigint))

    assert not datatype.DataTypeBigint.deduce_parser_config(None)
    assert not datatype.DataTypeBigint.deduce_parser_config("abc")
    assert not datatype.DataTypeBigint.deduce_parser_config(str(max_bigint + 1))
    assert not datatype.DataTypeBigint.deduce_parser_config(str(min_bigint - 1))

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.DataTypeBigint.deduce_parser_config(("41", "42", "43"))
    assert not datatype.DataTypeBigint.deduce_parser_config(
        decimal.Decimal('528676.842519685039')
    )


def test_parse_bigint():
    max_bigint = pow(2, 63) - 1

    parse_bigint = datatype.DataTypeBigint.string_parser(
        datatype.DataTypeBigint.string_parser_config({})
    )

    assert_raises(ValueError, parse_bigint, "abc")
    assert_raises(ValueError, parse_bigint, str(max_bigint + 1))

    assert parse_bigint(str(max_bigint)) == max_bigint
    assert parse_bigint("42") == 42


def test_matches_integer():
    min_integer = -pow(2, 31)
    max_integer = pow(2, 31) - 1

    assert datatype.DataTypeInteger.deduce_parser_config("10")
    assert not datatype.DataTypeInteger.deduce_parser_config("0,1,3,1,0")
    assert not datatype.DataTypeInteger.deduce_parser_config(12.4)
    assert datatype.DataTypeInteger.deduce_parser_config("-10")
    assert datatype.DataTypeInteger.deduce_parser_config(str(max_integer))
    assert datatype.DataTypeInteger.deduce_parser_config(str(min_integer))

    assert not datatype.DataTypeInteger.deduce_parser_config(None), \
        "Integer shouldn't accept None value"
    assert not datatype.DataTypeInteger.deduce_parser_config("abc"), \
        "Integer shouldn't accept alphabetic characters"
    assert not datatype.DataTypeInteger.deduce_parser_config(str(max_integer + 1)), \
        "Int32 shouldn't accept a value greater than %d" % max_integer
    assert not datatype.DataTypeInteger.deduce_parser_config(str(min_integer - 1)), \
        "Int32 shouldn't accept a value smaller than %d" % min_integer

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return None.
    assert not datatype.DataTypeInteger.deduce_parser_config(("41", "42", "43"))

    assert not datatype.DataTypeInteger.deduce_parser_config(decimal.Decimal('528676.842519685039'))


def test_parse_integer():
    max_integer = pow(2, 31) - 1

    parse_integer = datatype.DataTypeInteger.string_parser(
        datatype.DataTypeInteger.string_parser_config({})
    )

    assert_raises(ValueError, parse_integer, "abc")
    assert_raises(ValueError, parse_integer, str(max_integer + 1))

    assert parse_integer(str(max_integer)) == max_integer
    assert parse_integer("42") == 42
    assert parse_integer("-42") == -42


def test_matches_smallint():
    min_int16 = -pow(2, 15)
    max_int16 = pow(2, 15) - 1

    assert datatype.DataTypeSmallInt.deduce_parser_config("")
    assert datatype.DataTypeSmallInt.deduce_parser_config("10")
    assert datatype.DataTypeSmallInt.deduce_parser_config("-10")
    assert datatype.DataTypeSmallInt.deduce_parser_config(str(max_int16))
    assert datatype.DataTypeSmallInt.deduce_parser_config(str(min_int16))

    assert not datatype.DataTypeSmallInt.deduce_parser_config(None), \
        "smallint shouldn't accept None value"
    assert not datatype.DataTypeSmallInt.deduce_parser_config("abc"), \
        "Integer shouldn't accept alphabetic characters"
    assert not datatype.DataTypeSmallInt.deduce_parser_config(str(max_int16 + 1)), \
        "Int16 shouldn't accept a value greater than %d" % max_int16
    assert not datatype.DataTypeSmallInt.deduce_parser_config(str(min_int16 - 1)), \
        "Int16 shouldn't accept a value smaller than %d" % min_int16

    # Checking for a match with any other type than a string shouldn't
    # result in an exception, but just return None.
    assert not datatype.DataTypeSmallInt.deduce_parser_config(("41", "42", "43"))

    assert not datatype.DataTypeSmallInt.deduce_parser_config(decimal.Decimal('5.842519685039'))


def test_parse_smallint():
    max_smallint = pow(2, 15) - 1

    parse_smallint = datatype.DataTypeSmallInt.string_parser(
        datatype.DataTypeSmallInt.string_parser_config({})
    )

    assert_raises(ValueError, parse_smallint, "abc")
    assert_raises(ValueError, parse_smallint, str(max_smallint + 1))

    assert parse_smallint(str(max_smallint)) == max_smallint
    assert parse_smallint("42") == 42
    assert parse_smallint("-42") == -42


def test_matches_boolean():
    assert datatype.DataTypeBoolean.deduce_parser_config("0")
    assert datatype.DataTypeBoolean.deduce_parser_config("1")
    assert datatype.DataTypeBoolean.deduce_parser_config("True")
    assert datatype.DataTypeBoolean.deduce_parser_config("False")
    assert datatype.DataTypeBoolean.deduce_parser_config("true")
    assert datatype.DataTypeBoolean.deduce_parser_config("false")

    # Checking for a match with any other type than a string shouldn't
    # result in an exception, but just return None.
    assert not datatype.DataTypeBoolean.deduce_parser_config(("0", "1", "0"))
    assert not datatype.DataTypeBoolean.deduce_parser_config(None)


def test_parse_boolean():
    parse_boolean = datatype.DataTypeBoolean.string_parser(
        datatype.DataTypeBoolean.string_parser_config({})
    )

    assert_raises(
        datatype.ParseError,
        parse_boolean,
        "abc"
    )

    assert_raises(datatype.ParseError, parse_boolean, "2")

    parse_boolean = datatype.DataTypeBoolean.string_parser(
        datatype.DataTypeBoolean.string_parser_config({
            "true_value": ("1", "True", "true"),
            "false_value": ("0", "False", "false")
        })
    )

    assert parse_boolean("1")
    assert parse_boolean("True")
    assert parse_boolean("true")

    assert not parse_boolean("0")
    assert not parse_boolean("False")
    assert not parse_boolean("false")


def test_serialize_boolean():
    serialize_boolean = datatype.DataTypeBoolean.string_serializer()

    assert serialize_boolean(True) == 'true'

    serialize_boolean = datatype.DataTypeBoolean.string_serializer({
        'true_value': '1',
        'false_value': '0'
    })

    assert serialize_boolean(True) == '1'
    assert serialize_boolean(False) == '0'


def test_matches_double_precision():
    assert not datatype.DataTypeDoublePrecision.deduce_parser_config("abc")

    assert datatype.DataTypeDoublePrecision.deduce_parser_config("0.0")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("42")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("042")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("42.42")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("42e10")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("42.42e10")
    assert datatype.DataTypeDoublePrecision.deduce_parser_config("42.42e-10")

    # Checking for a match with any other type than a string
    # shouldn't result in a TypeError exception, but just return False.
    assert not datatype.DataTypeDoublePrecision.deduce_parser_config(None)
    assert not datatype.DataTypeDoublePrecision.deduce_parser_config(0.0)
    assert not datatype.DataTypeDoublePrecision.deduce_parser_config(42.0)
    assert not datatype.DataTypeDoublePrecision.deduce_parser_config(("42.41", "42.42", "42.43"))


def test_parse_double_precision():
    parse_double_precision = datatype.DataTypeDoublePrecision.string_parser(
        datatype.DataTypeDoublePrecision.string_parser_config({})
    )

    value = parse_double_precision("1.1")

    assert (value >= 1.0 and value <= 1.2)

    value = parse_double_precision("42.42")

    assert (value >= 42.41 and value <= 42.43)

    value = parse_double_precision("42e10")

    assert (value >= 419999999999.0 and value <= 420000000000.1)

    value = parse_double_precision("42.42e10")

    assert (value >= 424199999999.9 and value <= 424200000000.1)

    value = parse_double_precision("42.33e-10")

    assert (value >= 0.000000004232 and value <= 0.000000004234)


def test_matches_timestamp():
    assert not datatype.DataTypeTimestamp.deduce_parser_config("abc")

    parser_config = datatype.DataTypeTimestamp.deduce_parser_config(
        "2009-05-10 11:00:00"
    )

    assert parser_config['format'] == '%Y-%m-%d %H:%M:%S'

    assert not datatype.DataTypeTimestamp.deduce_parser_config(None)


def test_parse_timestamp():
    parse_timestamp = datatype.DataTypeTimestamp.string_parser(
        datatype.DataTypeTimestamp.string_parser_config({})
    )

    assert parse_timestamp("2009-05-10T11:00:00") == datetime(2009, 5, 10, 11, 0, 0)


def test_serialize_timestamp():
    timestamp = datetime(2009, 5, 10, 11, 0, 0)

    serialize_timestamp = datatype.DataTypeTimestamp.string_serializer()

    assert serialize_timestamp(timestamp) == "2009-05-10T11:00:00"


def test_matches_numeric():
    assert not datatype.DataTypeNumeric.deduce_parser_config("abc")
    assert datatype.DataTypeNumeric.deduce_parser_config("123.456")
    assert not datatype.DataTypeNumeric.deduce_parser_config("123,456")

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.DataTypeNumeric.deduce_parser_config(("123.456", "123.456", "123.456"))

    assert datatype.DataTypeNumeric.deduce_parser_config(
        decimal.Decimal('528676.842519685039')
    )

    assert datatype.DataTypeNumeric.deduce_parser_config(
        decimal.Decimal('5.842519685039')
    )


def test_parse_numeric():
    parse_numeric = datatype.DataTypeNumeric.string_parser(
        datatype.DataTypeNumeric.string_parser_config({})
    )

    assert_raises(datatype.ParseError, parse_numeric, "abc")

    value = parse_numeric("123.456")

    assert value == decimal.Decimal("123.456")


def test_deduce_from_string():
    parser_descriptor = datatype.parser_descriptor_from_string("")
    eq_(parser_descriptor.data_type.name, "smallint")

    parser_descriptor = datatype.parser_descriptor_from_string("100")
    eq_(parser_descriptor.data_type.name, "smallint")

    parser_descriptor = datatype.parser_descriptor_from_string("100000")
    eq_(parser_descriptor.data_type.name, "integer")

    parser_descriptor = datatype.parser_descriptor_from_string("10,89au")
    eq_(parser_descriptor.data_type.name, "text")

    parser_descriptor = datatype.parser_descriptor_from_string("30.0")
    eq_(parser_descriptor.data_type.name, "real")

    parser_descriptor = datatype.parser_descriptor_from_string("0.0")
    eq_(parser_descriptor.data_type.name, "real")


def test_max_datatypes():
    current_data_types = [
        (datatype.DataTypeSmallInt, {}),
        (datatype.DataTypeSmallInt, {})
    ]

    new_data_types = [
        (datatype.DataTypeInteger, {}),
        (datatype.DataTypeInteger, {})
    ]

    max_data_types = datatype.max_data_types(current_data_types, new_data_types)

    eq_(
        max_data_types, [
            (datatype.DataTypeInteger, {}),
            (datatype.DataTypeInteger, {})
        ]
    )

