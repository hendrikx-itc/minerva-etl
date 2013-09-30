# -*- coding: utf-8 -*-
"""
Unit tests for the core.datatype module
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import decimal

from nose.tools import eq_, assert_raises

from minerva.storage import datatype


def test_matches_string():
    assert datatype.matches_string("Some string that\nshould be\naccepted.")


def test_parse_string():
    value = "Some string that\nshould be\naccepted."

    assert datatype.parse_string(value) == value


def test_matches_bigint():
    min_bigint = -pow(2, 63)
    max_bigint = pow(2, 63) - 1

    assert datatype.matches_bigint("10")
    assert datatype.matches_bigint("-10")
    assert datatype.matches_bigint(str(max_bigint))
    assert datatype.matches_bigint(str(min_bigint))
    assert datatype.matches_bigint(None)

    assert not datatype.matches_bigint("abc")
    assert not datatype.matches_bigint(str(max_bigint + 1))
    assert not datatype.matches_bigint(str(min_bigint - 1))

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_bigint(("41", "42", "43"))

    assert not datatype.matches_bigint(decimal.Decimal('528676.842519685039'))


def test_parse_bigint():
    max_bigint = pow(2, 63) - 1

    assert_raises(ValueError, datatype.parse_bigint, "abc")
    assert_raises(ValueError, datatype.parse_bigint, str(max_bigint + 1))

    assert datatype.parse_bigint(str(max_bigint)) == max_bigint
    assert datatype.parse_bigint("42") == 42


def test_matches_integer():
    min_integer = -pow(2, 31)
    max_integer = pow(2, 31) - 1

    assert datatype.matches_integer("10")
    assert not datatype.matches_integer("0,1,3,1,0")
    assert not datatype.matches_integer(12.4)
    assert datatype.matches_integer("-10")
    assert datatype.matches_integer(str(max_integer))
    assert datatype.matches_integer(str(min_integer))
    assert datatype.matches_integer(None)

    assert not datatype.matches_integer("abc"), \
        "Integer shouldn't accept alphabetic characters"
    assert not datatype.matches_integer(str(max_integer + 1)), \
        "Int32 shouldn't accept a value greater than %d" % max_integer
    assert not datatype.matches_integer(str(min_integer - 1)), \
        "Int32 shouldn't accept a value smaller than %d" % min_integer

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_integer(("41", "42", "43"))

    assert not datatype.matches_integer(decimal.Decimal('528676.842519685039'))


def test_parse_integer():
    max_integer = pow(2, 31) - 1

    assert_raises(ValueError, datatype.parse_integer, "abc")
    assert_raises(ValueError, datatype.parse_integer, str(max_integer + 1))

    assert datatype.parse_integer(str(max_integer)) == max_integer
    assert datatype.parse_integer("42") == 42
    assert datatype.parse_integer("-42") == -42


def test_matches_smallint():
    min_int16 = -pow(2, 15)
    max_int16 = pow(2, 15) - 1

    assert datatype.matches_smallint("10")
    assert datatype.matches_smallint("-10")
    assert datatype.matches_smallint(str(max_int16))
    assert datatype.matches_smallint(str(min_int16))
    assert datatype.matches_smallint(None)

    assert not datatype.matches_smallint("abc"), \
        "Integer shouldn't accept alphabetic characters"
    assert not datatype.matches_smallint(str(max_int16 + 1)), \
        "Int16 shouldn't accept a value greater than %d" % max_int16
    assert not datatype.matches_smallint(str(min_int16 - 1)), \
        "Int16 shouldn't accept a value smaller than %d" % min_int16

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_smallint(("41", "42", "43"))

    assert not datatype.matches_smallint(decimal.Decimal('5.842519685039'))


def test_parse_smallint():
    max_smallint = pow(2, 15) - 1

    assert_raises(ValueError, datatype.parse_smallint, "abc")
    assert_raises(ValueError, datatype.parse_smallint, str(max_smallint + 1))

    assert datatype.parse_smallint(str(max_smallint)) == max_smallint
    assert datatype.parse_smallint("42") == 42
    assert datatype.parse_smallint("-42") == -42


def test_matches_boolean():
    assert datatype.matches_boolean("0")
    assert datatype.matches_boolean("1")
    assert datatype.matches_boolean("True")
    assert datatype.matches_boolean("False")
    assert datatype.matches_boolean("true")
    assert datatype.matches_boolean("false")
    assert datatype.matches_boolean(None)

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_boolean(("0", "1", "0"))


def test_parse_boolean():
    assert_raises(ValueError, datatype.parse_boolean, "abc")
    assert_raises(ValueError, datatype.parse_boolean, "2")

    assert datatype.parse_boolean("1")
    assert datatype.parse_boolean("True")
    assert datatype.parse_boolean("true")

    assert not datatype.parse_boolean("0")
    assert not datatype.parse_boolean("False")
    assert not datatype.parse_boolean("false")


def test_matches_float():
    assert not datatype.matches_float("abc")

    assert datatype.matches_float(0.0)
    assert datatype.matches_float(42.0)
    assert datatype.matches_float("0.0")
    assert datatype.matches_float("42")
    assert datatype.matches_float("042")
    assert datatype.matches_float("42.42")
    assert datatype.matches_float("42e10")
    assert datatype.matches_float("42.42e10")
    assert datatype.matches_float("42.42e-10")
    assert datatype.matches_float(None)

    # Checking for a match with any other type than a string or number
    # shouldn't result in a TypeError exception, but just return False.
    assert not datatype.matches_float(("42.41", "42.42", "42.43"))


def test_parse_float():
    value = datatype.parse_float("1.1")

    assert (value >= 1.0 and value <= 1.2)

    value = datatype.parse_float("42.42")

    assert (value >= 42.41 and value <= 42.43)

    value = datatype.parse_float("42e10")

    assert (value >= 419999999999.0 and value <= 420000000000.1)

    value = datatype.parse_float("42.42e10")

    assert (value >= 424199999999.9 and value <= 424200000000.1)

    value = datatype.parse_float("42.33e-10")

    assert (value >= 0.000000004232 and value <= 0.000000004234)


def test_matches_timestamp():
    assert not datatype.matches_timestamp("abc")
    assert datatype.matches_timestamp("2009-05-10 11:00:00")
    assert datatype.matches_timestamp(None)

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_timestamp(("2009-05-10 11:00:00", "2009-05-10 12:00:00", "2009-05-10 13:00:00"))


def test_parse_timestamp():
    value = datatype.parse_timestamp("2009-05-10 11:00:00")


def test_matches_decimal():
    assert not datatype.matches_decimal("abc")
    assert datatype.matches_decimal("123.456")
    assert not datatype.matches_decimal("123,456")

    # Checking for a match with any other type than a string shouldn't
    # result in a TypeError exception, but just return False.
    assert not datatype.matches_decimal(("123.456", "123.456", "123.456"))

    assert datatype.matches_decimal(decimal.Decimal('528676.842519685039'))
    assert datatype.matches_decimal(decimal.Decimal('5.842519685039'))


def test_parse_decimal():
    assert_raises(ValueError, datatype.parse_decimal, "abc")

    value = datatype.parse_decimal("123.456")

    assert value == decimal.Decimal("123.456")


def test_matches_array_of_smallints():
    assert datatype.matches_array_of_smallints([12,33,123,-3])
    assert datatype.matches_array_of_smallints("12,33,123,-3")
    assert datatype.matches_array_of_smallints("12")
    assert datatype.matches_array_of_smallints("12,33,,-3")
    assert datatype.matches_array_of_smallints((12,33,123,-3))

    assert not datatype.matches_array_of_smallints("")
    assert not datatype.matches_array_of_smallints("a,b")
    assert not datatype.matches_array_of_smallints("a,b")
    assert not datatype.matches_array_of_smallints([12,33,123,-3,234.33])
    assert not datatype.matches_array_of_smallints(1244)
    assert not datatype.matches_array_of_smallints(1244.34)


def test_matches_array_of_integers():
    assert datatype.matches_array_of_integers([12,33,123,-3])
    assert datatype.matches_array_of_integers("12,33,123,-3")
    assert datatype.matches_array_of_integers("12")
    assert datatype.matches_array_of_integers("12,33,,-3")
    assert datatype.matches_array_of_integers((12,33,123,-3))

    assert not datatype.matches_array_of_integers("")
    assert not datatype.matches_array_of_integers("a,b")
    assert not datatype.matches_array_of_integers("a,b")
    assert not datatype.matches_array_of_integers([12,33,123,-3,234.33])
    assert not datatype.matches_array_of_integers(1244)
    assert not datatype.matches_array_of_integers(1244.34)


def test_extract_from_value():
    eq_(datatype.extract_from_value("100"), "smallint")
    eq_(datatype.extract_from_value("100000"), "integer")
    eq_(datatype.extract_from_value("10,89"), "smallint[]")
    eq_(datatype.extract_from_value([10, 89]), "smallint[]")
    eq_(datatype.extract_from_value("1000000,89"), "integer[]")
    eq_(datatype.extract_from_value([1000000, 89]), "integer[]")
    eq_(datatype.extract_from_value(["eueou", "oeu"]), "text[]")
    eq_(datatype.extract_from_value("10,89au"), "text")
    eq_(datatype.extract_from_value(12.34), "real")
    eq_(datatype.extract_from_value(0.0), "real")
    eq_(datatype.extract_from_value(10.5), "real")
    eq_(datatype.extract_from_value(22.3), "real")
    eq_(datatype.extract_from_value(30.0), "real")
    eq_(datatype.extract_from_value("30.0"), "real")
    eq_(datatype.extract_from_value("0.0"), "real")
    eq_(datatype.extract_from_value(decimal.Decimal('528676.842519685039')),
        "numeric")
    eq_(datatype.extract_from_value(decimal.Decimal('6.842519685039')),
        "numeric")


def test_max_datatypes():
    current_data_types = ["smallint", "smallint"]
    new_data_types = ["integer", "integer"]

    max_datatypes = datatype.max_datatypes(current_data_types, new_data_types)

    eq_(max_datatypes, ["integer", "integer"])

