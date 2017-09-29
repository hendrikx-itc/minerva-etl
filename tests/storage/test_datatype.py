# -*- coding: utf-8 -*-
"""
Unit tests for the core.datatype module
"""
import decimal
from datetime import datetime
import unittest

from minerva.storage import datatype
from minerva.storage.datatype import ParseError


class TestDataTypeString(unittest.TestCase):
    def test_matches_string(self):
        self.assertTrue(datatype.registry['text'].deduce_parser_config(
            "Some string that\nshould be\naccepted."
        ))

    def test_parse_string(self):
        value = "Some string that\nshould be\naccepted."

        self.assertTrue(datatype.registry['text'].string_parser()(value))


class TestDataTypeBigInt(unittest.TestCase):
    def test_matches_bigint(self):
        min_bigint = -pow(2, 63)
        max_bigint = pow(2, 63) - 1

        self.assertIsNotNone(
            datatype.registry['bigint'].deduce_parser_config("10")
        )

        self.assertIsNotNone(
            datatype.registry['bigint'].deduce_parser_config("-10")
        )

        self.assertIsNotNone(
            datatype.registry['bigint'].deduce_parser_config(str(max_bigint))
        )

        self.assertIsNotNone(
            datatype.registry['bigint'].deduce_parser_config(str(min_bigint))
        )

        self.assertIsNone(
            datatype.registry['bigint'].deduce_parser_config(None)
        )

        self.assertFalse(
            datatype.registry['bigint'].deduce_parser_config("abc")
        )

        self.assertFalse(
            datatype.registry['bigint'].deduce_parser_config(
                str(max_bigint + 1)
            )
        )

        self.assertFalse(
            datatype.registry['bigint'].deduce_parser_config(
                str(min_bigint - 1)
            )
        )

        # Checking for a match with any other type than a string shouldn't
        # result in a TypeError exception, but just return False.
        self.assertFalse(
            datatype.registry['bigint'].deduce_parser_config(("41", "42", "43"))
        )
        self.assertFalse(
            datatype.registry['bigint'].deduce_parser_config(
                decimal.Decimal('528676.842519685039')
            )
        )

    def test_parse_bigint(self):
        max_bigint = pow(2, 63) - 1

        parse_bigint = datatype.registry['bigint'].string_parser()

        with self.assertRaises(ValueError):
            parse_bigint("abc")

        with self.assertRaises(ValueError):
            parse_bigint(str(max_bigint + 1))

        self.assertEqual(parse_bigint(str(max_bigint)), max_bigint)
        self.assertEqual(parse_bigint("42"), 42)


class TestDataTypeInteger(unittest.TestCase):
    def test_matches_integer(self):
        min_integer = -pow(2, 31)
        max_integer = pow(2, 31) - 1

        self.assertIsInstance(
            datatype.registry['integer'].deduce_parser_config("10"),
            dict
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config("0,1,3,1,0")
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(12.4)
        )

        self.assertIsInstance(
            datatype.registry['integer'].deduce_parser_config("-10"), dict
        )

        self.assertIsInstance(
            datatype.registry['integer'].deduce_parser_config(str(max_integer)),
            dict
        )

        self.assertIsInstance(
            datatype.registry['integer'].deduce_parser_config(str(min_integer)),
            dict
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(None),
            "Integer shouldn't accept None value"
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config("abc"),
            "Integer shouldn't accept alphabetic characters"
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(str(
                max_integer + 1)),
            "Int32 shouldn't accept a value greater than %d" % max_integer
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(str(
                min_integer - 1)),
            "Int32 shouldn't accept a value smaller than %d" % min_integer
        )

        # Checking for a match with any other type than a string shouldn't
        # result in a TypeError exception, but just return None.
        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(
                ("41", "42", "43")
            )
        )

        self.assertIsNone(
            datatype.registry['integer'].deduce_parser_config(
                decimal.Decimal('528676.842519685039')
            )
        )

    def test_parse_integer(self):
        max_integer = pow(2, 31) - 1

        parse_integer = datatype.registry['integer'].string_parser()

        with self.assertRaises(ValueError):
            parse_integer("abc")

        with self.assertRaises(ValueError):
            parse_integer(str(max_integer + 1))

        self.assertEqual(parse_integer(str(max_integer)), max_integer)
        self.assertEqual(parse_integer("42"), 42)
        self.assertEqual(parse_integer("-42"), -42)


class TestDataTypeSmallInt(unittest.TestCase):
    def test_matches_smallint(self):
        min_int16 = -pow(2, 15)
        max_int16 = pow(2, 15) - 1

        assert datatype.registry['smallint'].deduce_parser_config("")
        assert datatype.registry['smallint'].deduce_parser_config("10")
        assert datatype.registry['smallint'].deduce_parser_config("-10")
        assert datatype.registry['smallint'].deduce_parser_config(str(max_int16))
        assert datatype.registry['smallint'].deduce_parser_config(str(min_int16))

        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config(None),
            "smallint shouldn't accept None value"
        )
        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config("abc"),
            "Integer shouldn't accept alphabetic characters"
        )
        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config(str(max_int16 + 1)),
            "Int16 shouldn't accept a value greater than %d" % max_int16
        )
        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config(str(min_int16 - 1)),
            "Int16 shouldn't accept a value smaller than %d" % min_int16
        )

        # Checking for a match with any other type than a string shouldn't
        # result in an exception, but just return None.
        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config(("41", "42", "43"))
        )

        self.assertIsNone(
            datatype.registry['smallint'].deduce_parser_config(
                decimal.Decimal('5.842519685039')
            )
        )

    def test_parse_smallint(self):
        max_smallint = pow(2, 15) - 1

        parse_smallint = datatype.registry['smallint'].string_parser({})

        with self.assertRaises(ValueError):
            parse_smallint("abc")

        with self.assertRaises(ValueError):
            parse_smallint(str(max_smallint + 1))

        self.assertEqual(parse_smallint(str(max_smallint)), max_smallint)
        self.assertEqual(parse_smallint("42"), 42)
        self.assertEqual(parse_smallint("-42"), -42)


class TestDataTypeBoolean(unittest.TestCase):
    def test_matches_boolean(self):
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("0"), dict
        )
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("1"), dict
        )
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("True"), dict
        )
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("False"), dict
        )
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("true"), dict
        )
        self.assertIsInstance(
            datatype.registry['boolean'].deduce_parser_config("false"), dict
        )

        # Checking for a match with any other type than a string shouldn't
        # result in an exception, but just return None.
        self.assertIsNone(
            datatype.registry['boolean'].deduce_parser_config(("0", "1", "0"))
        )
        self.assertIsNone(
            datatype.registry['boolean'].deduce_parser_config(None)
        )

    def test_parse_boolean(self):
        parse_boolean = datatype.registry['boolean'].string_parser()

        with self.assertRaises(ParseError):
            parse_boolean("abc")

        with self.assertRaises(ParseError):
            parse_boolean("2")

        parse_boolean = datatype.registry['boolean'].string_parser({
            "true_value": ("1", "True", "true"),
            "false_value": ("0", "False", "false")
        })

        self.assertEqual(parse_boolean("1"), True)
        self.assertEqual(parse_boolean("True"), True)
        self.assertEqual(parse_boolean("true"), True)

        self.assertEqual(parse_boolean("0"), False)
        self.assertEqual(parse_boolean("False"), False)
        self.assertEqual(parse_boolean("false"), False)

    def test_serialize_boolean(self):
        serialize_boolean = datatype.registry['boolean'].string_serializer()

        self.assertEqual(serialize_boolean(True), 'true')

        serialize_boolean = datatype.registry['boolean'].string_serializer({
            'true_value': '1',
            'false_value': '0'
        })

        self.assertEqual(serialize_boolean(True), '1')
        self.assertEqual(serialize_boolean(False), '0')


class TestDataTypeDoublePrecision(unittest.TestCase):
    def test_matches_double_precision(self):
        self.assertIsNone(
            datatype.registry['double precision'].deduce_parser_config("abc")
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("0.0"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("42"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("042"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("42.42"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("42e10"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("42.42e10"),
            dict
        )

        self.assertIsInstance(
            datatype.registry['double precision'].deduce_parser_config("42.42e-10"),
            dict
        )

        # Checking for a match with any other type than a string
        # shouldn't result in a TypeError exception, but just return None.
        self.assertIsNone(
            datatype.registry['double precision'].deduce_parser_config(None)
        )
        self.assertIsNone(
            datatype.registry['double precision'].deduce_parser_config(0.0)
        )
        self.assertIsNone(
            datatype.registry['double precision'].deduce_parser_config(42.0)
        )
        self.assertIsNone(
            datatype.registry['double precision'].deduce_parser_config(
                ("42.41", "42.42", "42.43")
            )
        )

    def test_parse_double_precision(self):
        parse_double_precision = datatype.registry[
                'double precision'].string_parser({})

        value = parse_double_precision("1.1")

        self.assertTrue(1.0 <= value <= 1.2)

        value = parse_double_precision("42.42")

        self.assertTrue(42.41 <= value <= 42.43)

        value = parse_double_precision("42e10")

        self.assertTrue(419999999999.0 <= value <= 420000000000.1)

        value = parse_double_precision("42.42e10")

        self.assertTrue(424199999999.9 <= value <= 424200000000.1)

        value = parse_double_precision("42.33e-10")

        self.assertTrue(0.000000004232 <= value <= 0.000000004234)


class TestDataTypeDoubleTimestamp(unittest.TestCase):
    def test_matches_timestamp(self):
        self.assertIsNone(
            datatype.registry['timestamp'].deduce_parser_config("abc")
        )

        parser_config = datatype.registry['timestamp'].deduce_parser_config(
            "2009-05-10 11:00:00"
        )

        self.assertEqual(parser_config['format'], '%Y-%m-%d %H:%M:%S')

        self.assertIsNone(
            datatype.registry['timestamp'].deduce_parser_config(None)
        )

    def test_parse_timestamp(self):
        parse_timestamp = datatype.registry['timestamp'].string_parser()

        self.assertEqual(
            parse_timestamp("2009-05-10T11:00:00"),
            datetime(2009, 5, 10, 11, 0, 0)
        )

    def test_serialize_timestamp(self):
        timestamp = datetime(2009, 5, 10, 11, 0, 0)

        serialize_timestamp = datatype.registry['timestamp'].string_serializer()

        self.assertEqual(serialize_timestamp(timestamp), "2009-05-10T11:00:00")


class TestDataTypeNumeric(unittest.TestCase):
    def test_matches_numeric(self):
        self.assertIsNone(
            datatype.registry['numeric'].deduce_parser_config("abc")
        )

        self.assertIsInstance(
            datatype.registry['numeric'].deduce_parser_config("123.456"),
            dict
        )

        self.assertIsNone(
            datatype.registry['numeric'].deduce_parser_config("123,456")
        )

        # Checking for a match with any other type than a string shouldn't
        # result in a TypeError exception, but just return False.
        self.assertIsNone(datatype.registry['numeric'].deduce_parser_config(
            ("123.456", "123.456", "123.456")
        ))

        self.assertIsInstance(
            datatype.registry['numeric'].deduce_parser_config(
                decimal.Decimal('528676.842519685039')
            ),
            dict
        )

        self.assertIsInstance(
            datatype.registry['numeric'].deduce_parser_config(
                decimal.Decimal('5.842519685039')
            ),
            dict
        )

    def test_parse_numeric(self):
        parse_numeric = datatype.registry['numeric'].string_parser()

        with self.assertRaises(ParseError):
            parse_numeric("abc")

        value = parse_numeric("123.456")

        self.assertEqual(value, decimal.Decimal("123.456"))


class TestDataTypeUtils(unittest.TestCase):
    def test_deduce_from_string(self):
        parser_descriptor = datatype.parser_descriptor_from_string("")
        self.assertEqual(parser_descriptor.data_type.name, "smallint")

        parser_descriptor = datatype.parser_descriptor_from_string("100")
        self.assertEqual(parser_descriptor.data_type.name, "smallint")

        parser_descriptor = datatype.parser_descriptor_from_string("100000")
        self.assertEqual(parser_descriptor.data_type.name, "integer")

        parser_descriptor = datatype.parser_descriptor_from_string("10,89au")
        self.assertEqual(parser_descriptor.data_type.name, "text")

        parser_descriptor = datatype.parser_descriptor_from_string("30.0")
        self.assertEqual(parser_descriptor.data_type.name, "real")

        parser_descriptor = datatype.parser_descriptor_from_string("0.0")
        self.assertEqual(parser_descriptor.data_type.name, "real")

    def test_max_data_types(self):
        current_data_types = [
            datatype.registry['smallint'],
            datatype.registry['smallint']
        ]

        new_data_types = [
            datatype.registry['integer'],
            datatype.registry['integer']
        ]

        max_data_types = datatype.max_data_types(
                current_data_types, new_data_types
        )

        self.assertEqual(
            max_data_types,
            [
                datatype.registry['integer'],
                datatype.registry['integer']
            ]
        )

    def test_array_of_integer(self):
        arr_int = datatype.registry['integer[]']

        parser = arr_int.string_parser()

        value = parser('[1,2,3]')

        self.assertEqual(value, [1, 2, 3])

        value = parser('[1, 2, 3]')

        self.assertEqual(value, [1, 2, 3])

    def test_array_of_text(self):
        arr_text = datatype.registry['text[]']

        parser = arr_text.string_parser({})

        value = parser('[foo,bar,baz]')

        self.assertEqual(value, ['foo', 'bar', 'baz'])

    def test_type_registry(self):
        self.assertIn('integer', datatype.registry)

        self.assertIn('integer[]', datatype.registry)

        self.assertIn('timestamp with time zone', datatype.registry)

        self.assertIn('timestamp with time zone[]', datatype.registry)

    def test_deduce_data_types(self):
        rows = [
            ['10', 'x']
        ]

        data_types = datatype.deduce_data_types(rows)

        self.assertEqual(data_types[0], datatype.registry['smallint'])

        self.assertEqual(data_types[1], datatype.registry['text'])

    def test_compare_data_types(self):
        self.assertEqual(
            datatype.registry['smallint'],
            datatype.registry['smallint']
        )

        self.assertEqual(
            datatype.registry['smallint[]'],
            datatype.registry['smallint[]']
        )
