# -*- coding: utf-8 -*-
"""
Defines the data types recognized by Minerva.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import re
from datetime import datetime
from operator import truth
import decimal

INT64_MIN = -pow(2, 63)
INT64_MAX = pow(2, 63) - 1

INT32_MIN = -pow(2, 31)
INT32_MAX = pow(2, 31) - 1

INT16_MIN = -pow(2, 15)
INT16_MAX = pow(2, 15) - 1

TRUE_SET = set(["1", "True", "true"])
FALSE_SET = set(["0", "False", "false"])
BOOL_SET = TRUE_SET | FALSE_SET


def matches_string(value):
    return True


def parse_string(value):
    if hasattr(value, "__iter__"):
        return ";".join(value)

    return value


smallint_regex = re.compile("-?[1-9][0-9]*")


def matches_smallint(value):
    if value == "":
        return True

    if type(value) is float:
        return False

    if type(value) is decimal.Decimal:
        return False

    if type(value) is str and not smallint_regex.match(value):
        return False

    if value is None:
        return True

    try:
        int_val = int(value)
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return int_val >= INT16_MIN and int_val <= INT16_MAX


def parse_smallint(value):
    if not value:
        return None

    int_val = int(value)

    if not (int_val >= INT16_MIN and int_val <= INT16_MAX):
        raise ValueError("{0:d} is not in range {1:d} - {2:d}".format(
            int_val, INT16_MIN, INT16_MAX))

    return int_val


def matches_integer(value):
    if type(value) is float:
        return False

    if type(value) is decimal.Decimal:
        return False

    if value is None:
        return True

    try:
        int_val = int(value)
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return int_val >= INT32_MIN and int_val <= INT32_MAX


def parse_integer(value):
    if not value:
        return None

    int_val = int(value)

    if not (int_val >= INT32_MIN and int_val <= INT32_MAX):
        raise ValueError("{0:d} is not in range {1:d} - {2:d}".format(
            int_val, INT32_MIN, INT32_MAX))

    return int_val


def matches_bigint(value):
    if type(value) is float:
        return False

    if type(value) is decimal.Decimal:
        return False

    if value is None:
        return True

    try:
        int_val = int(value)
    except (TypeError, ValueError):
        return False
    else:
        return int_val >= INT64_MIN and int_val <= INT64_MAX


def parse_bigint(value):
    if not value:
        return None

    int_val = int(value)

    if not (int_val >= INT64_MIN and int_val <= INT64_MAX):
        raise ValueError("{0:d} is not in range {1:d} - {2:d}".format(
            int_val, INT64_MIN, INT64_MAX))

    return int_val


def matches_boolean(value):
    if value is None:
        return True

    return value in BOOL_SET


def parse_boolean(value):
    if not value:
        return None
    elif value in FALSE_SET:
        return False
    elif value in TRUE_SET:
        return True
    else:
        raise ValueError()


def matches_float(value):
    if type(value) is decimal.Decimal:
        return False

    if value is None or type(value) is float or len(value) == 0:
        return True

    try:
        float(value)
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return True


def parse_float(value):
    if not value:
        return None

    return float(value)


TIMESTAMP_REGEX = re.compile("^([0-9]{4})-([0-9]{2})-([0-9]{2}) \
([0-9]{2}):([0-9]{2}):([0-9]{2})$")


def matches_timestamp(value):
    if value is None:
        return True

    try:
        match = TIMESTAMP_REGEX.match(value)
    except TypeError:
        return False
    else:
        if match is not None:
            return True
        else:
            return False


def parse_timestamp(value):
    if not value:
        return None

    match = TIMESTAMP_REGEX.match(value)

    if match is None:
        raise ValueError()

    (year, month, date, hour, minute, second) = match.groups()

    datetime_val = datetime(int(year), int(month), int(date), int(hour),
                            int(minute), int(second))

    return datetime_val


def matches_array_of_smallints(value):
    """
    Returns True when value is comma separated string of small ints
    """
    if not value:
        return None

    if type(value) is list or type(value) is tuple:
        values = value
    elif type(value) is str:
        values = value.split(",")
    else:
        return False

    ints = filter(truth, values)

    return all(map(matches_smallint, ints))


def matches_array_of_integers(value):
    """
    Returns True when value is comma separated string of integers
    """
    if not value:
        return None

    if type(value) is list or type(value) is tuple:
        values = value
    elif type(value) is str:
        values = value.split(",")
    else:
        return False

    ints = filter(truth, values)

    return all(map(matches_integer, ints))


def matches_array_of_bigints(value):
    """
    Returns True when value is comma separated string of bigints
    """
    if not value:
        return None

    try:
        ints = map(int, (v for v in value.split(",") if v))
    except ValueError:
        return False

    try:
        return matches_bigint(max(ints))
    except ValueError:
        return True
    else:
        return False


def matches_array_of_strings(value):
    if value is None:
        return None

    if type(value) is list or type(value) is tuple:
        return True
    else:
        return False


def matches_decimal(value):
    try:
        decimal.Decimal(value)
    except decimal.InvalidOperation:
        return False
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return True


def parse_decimal(value):
    if not value:
        return None

    try:
        decimal_val = decimal.Decimal(value)
    except decimal.InvalidOperation:
        raise ValueError()

    return decimal_val


def parse_bits(value):
    try:
        return int(value, 2)
    except:
        raise ValueError()


def matches_bits(value):
    try:
        int(value, 2)
    except:
        return False
    else:
        return True


MATCH_FUNCS_BY_TYPE = {
    "text": matches_string,
    "smallint": matches_smallint,
    "integer": matches_integer,
    "bigint": matches_bigint,
    "real": matches_float,
    "double precision": matches_float,
    "numeric": matches_decimal,
    "timestamp without time zone": matches_timestamp,
    "bit varying": matches_bits,
    "smallint[]": matches_array_of_smallints,
    "integer[]": matches_array_of_integers,
    "bigint[]": matches_array_of_bigints,
    "text[]": matches_array_of_strings}


ALL_TYPES = [
    "text",
    "bigint[]",
    "integer[]",
    "smallint[]",
    "bigint",
    "integer",
    "smallint",
    "boolean",
    "real",
    "double precision",
    "timestamp without time zone",
    "numeric"]

# The set of type ids of types that are integer
INTEGER_TYPES = set([
    "bigint",
    "integer",
    "smallint"])

TYPE_ORDER = [
    "smallint",
    "integer",
    "bigint",
    "real",
    "double precision",
    "numeric",
    "timestamp without time zone",
    "smallint[]",
    "integer[]",
    "text[]",
    "text"]


TYPE_ORDER_RANKS = dict((data_type, i)
                        for i, data_type in enumerate(TYPE_ORDER))


def max_datatype(left, right):
    if TYPE_ORDER_RANKS[right] > TYPE_ORDER_RANKS[left]:
        return right
    else:
        return left


def max_datatypes(current_datatypes, new_datatypes):
    return [max_datatype(current_datatype, new_datatype)
            for current_datatype, new_datatype
            in zip(current_datatypes, new_datatypes)]


ORDERED_MATCH_FUNCS = [(data_type, MATCH_FUNCS_BY_TYPE[data_type])
                       for data_type in TYPE_ORDER]


def deduce_from_value(value):
    for data_type, match_func in ORDERED_MATCH_FUNCS:
        if match_func(value):
            return data_type

    raise ValueError("Unable to determine data type of: {0}".format(value))


extract_from_value = deduce_from_value
