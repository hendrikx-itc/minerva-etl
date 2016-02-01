# -*- coding: utf-8 -*-
"""
Unit tests for the core.datatype module
"""
import decimal
from datetime import datetime

from nose.tools import assert_equal, assert_is_not_none, assert_is_none, \
    assert_raises, eq_

from minerva.storage import datatype


def test_matches_string():
    assert datatype.Text.deduce_parser_config(
        "Some string that\nshould be\naccepted."
    )


def test_parse_string():
    value = "Some string that\nshould be\naccepted."

    assert datatype.Text.string_parser()(value) == value


def test_matches_bigint():
    min_bigint = -pow(2, 63)
    max_bigint = pow(2, 63) - 1

    assert_is_not_none(datatype.Bigint.deduce_parser_config("10"))
    assert_is_not_none(datatype.Bigint.deduce_parser_config("-10"))
    assert_is_not_none(
        datatype.Bigint.deduce_parser_config(str(max_bigint))
    )
    assert_is_not_none(
        datatype.Bigint.deduce_parser_config(str(min_bigint))
    )

    assert_is_none(datatype.Bigint.deduce_parser_config(None))
    assert not datatype.Bigint.deduce_parser_config("abc")
    assert not datatype.Bigint.deduce_parser_config(str(max_bigint + 1))
    assert not datatype.Bigint.deduce_parser_config(str(min_bigint - 1))

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.Bigint.deduce_parser_config(("41", "42", "43"))
    assert not datatype.Bigint.deduce_parser_config(
        decimal.Decimal('528676.842519685039')
    )


def test_parse_bigint():
    max_bigint = pow(2, 63) - 1

    parse_bigint = datatype.Bigint.string_parser()

    assert_raises(ValueError, parse_bigint, "abc")
    assert_raises(ValueError, parse_bigint, str(max_bigint + 1))

    assert parse_bigint(str(max_bigint)) == max_bigint
    assert parse_bigint("42") == 42


def test_matches_integer():
    min_integer = -pow(2, 31)
    max_integer = pow(2, 31) - 1

    assert datatype.Integer.deduce_parser_config("10")
    assert not datatype.Integer.deduce_parser_config("0,1,3,1,0")
    assert not datatype.Integer.deduce_parser_config(12.4)
    assert datatype.Integer.deduce_parser_config("-10")
    assert datatype.Integer.deduce_parser_config(str(max_integer))
    assert datatype.Integer.deduce_parser_config(str(min_integer))

    assert_is_none(
        datatype.Integer.deduce_parser_config(None),
        "Integer shouldn't accept None value"
    )
    assert_is_none(
        datatype.Integer.deduce_parser_config("abc"),
        "Integer shouldn't accept alphabetic characters"
    )
    assert_is_none(
        datatype.Integer.deduce_parser_config(str(max_integer + 1)),
        "Int32 shouldn't accept a value greater than %d" % max_integer
    )
    assert_is_none(
        datatype.Integer.deduce_parser_config(str(min_integer - 1)),
        "Int32 shouldn't accept a value smaller than %d" % min_integer
    )

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return None.
    assert not datatype.Integer.deduce_parser_config(("41", "42", "43"))

    assert not datatype.Integer.deduce_parser_config(
        decimal.Decimal('528676.842519685039')
    )


def test_parse_integer():
    max_integer = pow(2, 31) - 1

    parse_integer = datatype.Integer.string_parser()

    assert_raises(ValueError, parse_integer, "abc")
    assert_raises(ValueError, parse_integer, str(max_integer + 1))

    assert parse_integer(str(max_integer)) == max_integer
    assert parse_integer("42") == 42
    assert parse_integer("-42") == -42


def test_matches_smallint():
    min_int16 = -pow(2, 15)
    max_int16 = pow(2, 15) - 1

    assert datatype.SmallInt.deduce_parser_config("")
    assert datatype.SmallInt.deduce_parser_config("10")
    assert datatype.SmallInt.deduce_parser_config("-10")
    assert datatype.SmallInt.deduce_parser_config(str(max_int16))
    assert datatype.SmallInt.deduce_parser_config(str(min_int16))

    assert_is_none(
        datatype.SmallInt.deduce_parser_config(None),
        "smallint shouldn't accept None value"
    )
    assert_is_none(
        datatype.SmallInt.deduce_parser_config("abc"),
        "Integer shouldn't accept alphabetic characters"
    )
    assert_is_none(
        datatype.SmallInt.deduce_parser_config(str(max_int16 + 1)),
        "Int16 shouldn't accept a value greater than %d" % max_int16
    )
    assert_is_none(
        datatype.SmallInt.deduce_parser_config(str(min_int16 - 1)),
        "Int16 shouldn't accept a value smaller than %d" % min_int16
    )

    # Checking for a match with any other type than a string shouldn't
    # result in an exception, but just return None.
    assert_is_none(
        datatype.SmallInt.deduce_parser_config(("41", "42", "43"))
    )

    assert_is_none(
        datatype.SmallInt.deduce_parser_config(
            decimal.Decimal('5.842519685039')
        )
    )


def test_parse_smallint():
    max_smallint = pow(2, 15) - 1

    parse_smallint = datatype.SmallInt.string_parser({})

    assert_raises(ValueError, parse_smallint, "abc")
    assert_raises(ValueError, parse_smallint, str(max_smallint + 1))

    assert parse_smallint(str(max_smallint)) == max_smallint
    assert parse_smallint("42") == 42
    assert parse_smallint("-42") == -42


def test_matches_boolean():
    assert datatype.Boolean.deduce_parser_config("0")
    assert datatype.Boolean.deduce_parser_config("1")
    assert datatype.Boolean.deduce_parser_config("True")
    assert datatype.Boolean.deduce_parser_config("False")
    assert datatype.Boolean.deduce_parser_config("true")
    assert datatype.Boolean.deduce_parser_config("false")

    # Checking for a match with any other type than a string shouldn't
    # result in an exception, but just return None.
    assert not datatype.Boolean.deduce_parser_config(("0", "1", "0"))
    assert not datatype.Boolean.deduce_parser_config(None)


def test_parse_boolean():
    parse_boolean = datatype.Boolean.string_parser(
        datatype.Boolean.string_parser_config({})
    )

    assert_raises(
        datatype.ParseError,
        parse_boolean,
        "abc"
    )

    assert_raises(datatype.ParseError, parse_boolean, "2")

    parse_boolean = datatype.Boolean.string_parser({
        "true_value": ("1", "True", "true"),
        "false_value": ("0", "False", "false")
    })

    assert parse_boolean("1")
    assert parse_boolean("True")
    assert parse_boolean("true")

    assert not parse_boolean("0")
    assert not parse_boolean("False")
    assert not parse_boolean("false")


def test_serialize_boolean():
    serialize_boolean = datatype.Boolean.string_serializer()

    assert serialize_boolean(True) == 'true'

    serialize_boolean = datatype.Boolean.string_serializer({
        'true_value': '1',
        'false_value': '0'
    })

    assert serialize_boolean(True) == '1'
    assert serialize_boolean(False) == '0'


def test_matches_double_precision():
    assert not datatype.DoublePrecision.deduce_parser_config("abc")

    assert datatype.DoublePrecision.deduce_parser_config("0.0")
    assert datatype.DoublePrecision.deduce_parser_config("42")
    assert datatype.DoublePrecision.deduce_parser_config("042")
    assert datatype.DoublePrecision.deduce_parser_config("42.42")
    assert datatype.DoublePrecision.deduce_parser_config("42e10")
    assert datatype.DoublePrecision.deduce_parser_config("42.42e10")
    assert datatype.DoublePrecision.deduce_parser_config("42.42e-10")

    # Checking for a match with any other type than a string
    # shouldn't result in a TypeError exception, but just return False.
    assert not datatype.DoublePrecision.deduce_parser_config(None)
    assert not datatype.DoublePrecision.deduce_parser_config(0.0)
    assert not datatype.DoublePrecision.deduce_parser_config(42.0)
    assert not datatype.DoublePrecision.deduce_parser_config(
        ("42.41", "42.42", "42.43")
    )


def test_parse_double_precision():
    parse_double_precision = datatype.DoublePrecision.string_parser({})

    value = parse_double_precision("1.1")

    assert (1.0 <= value <= 1.2)

    value = parse_double_precision("42.42")

    assert (42.41 <= value <= 42.43)

    value = parse_double_precision("42e10")

    assert (419999999999.0 <= value <= 420000000000.1)

    value = parse_double_precision("42.42e10")

    assert (424199999999.9 <= value <= 424200000000.1)

    value = parse_double_precision("42.33e-10")

    assert (0.000000004232 <= value <= 0.000000004234)


def test_matches_timestamp():
    assert not datatype.Timestamp.deduce_parser_config("abc")

    parser_config = datatype.Timestamp.deduce_parser_config(
        "2009-05-10 11:00:00"
    )

    assert parser_config['format'] == '%Y-%m-%d %H:%M:%S'

    assert not datatype.Timestamp.deduce_parser_config(None)


def test_parse_timestamp():
    parse_timestamp = datatype.Timestamp.string_parser()

    eq_(
        parse_timestamp("2009-05-10T11:00:00"),
        datetime(2009, 5, 10, 11, 0, 0)
    )


def test_serialize_timestamp():
    timestamp = datetime(2009, 5, 10, 11, 0, 0)

    serialize_timestamp = datatype.Timestamp.string_serializer()

    assert serialize_timestamp(timestamp) == "2009-05-10T11:00:00"


def test_matches_numeric():
    assert not datatype.Numeric.deduce_parser_config("abc")
    assert datatype.Numeric.deduce_parser_config("123.456")
    assert not datatype.Numeric.deduce_parser_config("123,456")

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.Numeric.deduce_parser_config(
        ("123.456", "123.456", "123.456")
    )

    assert datatype.Numeric.deduce_parser_config(
        decimal.Decimal('528676.842519685039')
    )

    assert datatype.Numeric.deduce_parser_config(
        decimal.Decimal('5.842519685039')
    )


def test_parse_numeric():
    parse_numeric = datatype.Numeric.string_parser()

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


def test_max_data_types():
    current_data_types = [
        datatype.SmallInt,
        datatype.SmallInt
    ]

    new_data_types = [
        datatype.Integer,
        datatype.Integer
    ]

    max_data_types = datatype.max_data_types(current_data_types, new_data_types)

    eq_(
        max_data_types, [
            datatype.Integer,
            datatype.Integer
        ]
    )


def test_array_of_integer():
    arr_int = datatype.array_of(datatype.Integer)

    parser = arr_int.string_parser()

    value = parser('[1,2,3]')

    eq_(value, [1, 2, 3])

    value = parser('[1, 2, 3]')

    eq_(value, [1, 2, 3])


def test_array_of_text():
    arr_text = datatype.array_of(datatype.Text)

    parser = arr_text.string_parser({})

    value = parser('[foo,bar,baz]')

    eq_(value, ['foo', 'bar', 'baz'])


def test_type_map():
    assert 'integer' in datatype.type_map

    assert 'integer[]' in datatype.type_map

    assert 'timestamp with time zone' in datatype.type_map

    assert 'timestamp with time zone[]' in datatype.type_map


def test_deduce_data_types():
    rows = [
        ['10', 'x']
    ]

    data_types = datatype.deduce_data_types(rows)

    assert_equal(data_types[0], datatype.SmallInt)

    assert_equal(data_types[1], datatype.Text)
