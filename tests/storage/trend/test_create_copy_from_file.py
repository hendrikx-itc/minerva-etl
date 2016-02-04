# -*- coding: utf-8 -*-
import datetime

import pytz

from minerva.directory import DataSource
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.outputdescriptor import OutputDescriptor
from minerva.storage.trend.tabletrendstore import create_copy_from_file


DATA_SOURCE = DataSource(
    1, name="DummySource", description="Dummy data source"
)


def test_create_copy_from_file_empty():
    f = create_copy_from_file(None, None, [], [])

    assert f.read() == ""


def test_create_copy_from_file_simple():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23'))
    ]

    output_descriptors = [
        OutputDescriptor(ValueDescriptor('x', datatype.registry['text'])),
        OutputDescriptor(ValueDescriptor('y', datatype.registry['text']))
    ]

    serializers = [
        output_descriptor.serialize
        for output_descriptor in output_descriptors
    ]

    f = create_copy_from_file(timestamp, modified, rows, serializers)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "a\t23\n"
    )

    assert text == expected


def test_create_copy_from_file_int_array():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23', [1, 2, 3]))
    ]

    output_descriptors = [
        OutputDescriptor(
            ValueDescriptor('x', datatype.registry['text']),
            datatype.copy_from_serializer_config(datatype.registry['text'])
        ),
        OutputDescriptor(
            ValueDescriptor('y', datatype.registry['text']),
            datatype.copy_from_serializer_config(datatype.registry['text'])
        ),
        OutputDescriptor(
            ValueDescriptor('z', datatype.registry['smallint[]']),
            datatype.copy_from_serializer_config(
                datatype.registry['smallint[]']
            )
        )
    ]

    serializers = [
        output_descriptor.serialize
        for output_descriptor in output_descriptors
    ]

    f = create_copy_from_file(timestamp, modified, rows, serializers)

    text = f.read()
    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "\"a\"\t\"23\"\t{1,2,3}\n"
    )

    assert text == expected, '\n{}\n!=\n{}'.format(text, expected)


def test_create_copy_from_file_text_array():
    timestamp = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )
    modified = pytz.utc.localize(
        datetime.datetime(2008, 12, 3, 0, 15, 0)
    )

    rows = [
        (1, ('a', '23', ["a=b,c=d", "e=f,g=h", "i=j,k=l"]))
    ]

    output_descriptors = [
        OutputDescriptor(
            ValueDescriptor('x', datatype.registry['text']),
            datatype.copy_from_serializer_config(datatype.registry['text'])
        ),
        OutputDescriptor(
            ValueDescriptor('y', datatype.registry['smallint']),
            datatype.copy_from_serializer_config(datatype.registry['smallint'])
        ),
        OutputDescriptor(
            ValueDescriptor('z', datatype.registry['text[]']),
            datatype.copy_from_serializer_config(datatype.registry['text[]'])
        )
    ]

    serializers = [
        output_descriptor.serialize
        for output_descriptor in output_descriptors
    ]

    f = create_copy_from_file(timestamp, modified, rows, serializers)

    text = f.read()

    expected = (
        "1\t'2008-12-03T00:15:00+00:00'\t'2008-12-03T00:15:00+00:00'\t"
        "\"a\"\t23\t{\"a=b,c=d\",\"e=f,g=h\",\"i=j,k=l\"}\n"
    )

    assert text == expected, '\n{}\n!=\n{}'.format(text, expected)
