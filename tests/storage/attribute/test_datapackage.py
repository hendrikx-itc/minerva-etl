# -*- coding: utf-8 -*-
"""Tests for methods of the DataPackage class."""
from datetime import datetime
import unittest

import pytz

from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.outputdescriptor import OutputDescriptor
from minerva.storage.attribute.datapackage import DataPackage

TIMESTAMP = pytz.utc.localize(datetime(2013, 8, 30, 15, 30))


simple_package = DataPackage(
    ["power", "height", "state", "remark"],
    [
        (123001, TIMESTAMP, ("405", "0.0", "enabled", "")),
        (123003, TIMESTAMP, ("41033", "22.3", "enabled", "")),
        (123004, TIMESTAMP, ("880", "30.0", "enabled", ""))
    ]
)


array_package = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, ([0, 1, 2, 4, 7, 4, 2, 1, 0],)),
        (123002, TIMESTAMP, ([0, 1, 2, 5, 8, 4, 2, 1, 0],)),
        (123003, TIMESTAMP, ([0, 1, 3, 5, 7, 4, 3, 1, 0],)),
        (123004, TIMESTAMP, ([0, 1, 2, 4, 9, 4, 2, 1, 0],))
    ]
)


package_array_list_a = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, ([0, 1, 2],)),
        (123002, TIMESTAMP, ([0, 1, 2],)),
        (123003, TIMESTAMP, ([None, None, None],))
    ]
)


package_array_list_b = DataPackage(
    ["curve"],
    [
        (123001, TIMESTAMP, (['', ''],)),
        (123002, TIMESTAMP, (['', ''],))
    ]
)


package_array_list_c = DataPackage(
    ["curve"],
    [
        (123001, (['e=34,c=1', 'e=45,c=3', 'e=33,c=2'],)),
        (123002, (['', '', ''],))
    ]
)


class TestDataPackage(unittest.TestCase):
    def test_constructor(self):
        """Test creation of a new DataPackage instance."""
        data_package = simple_package
    
        self.assertEqual(len(data_package.attribute_names), 4)
        self.assertEqual(len(data_package.rows), 3)
    
    def test_deduce_value_descriptors(self):
        """The max data types should be deduced from the package."""
        data_package = DataPackage(
            ["power", "height", "state", "remark"],
            [
                (123001, TIMESTAMP, ("405", "0.0", "enabled", "")),
                (123003, TIMESTAMP, ("41033", "22.3", "enabled", "")),
                (123004, TIMESTAMP, ("880", "30.0", "enabled", ""))
            ]
        )
    
        value_descriptors = data_package.deduce_value_descriptors()
    
        self.assertEqual(
            value_descriptors[0],
            ValueDescriptor("power", datatype.registry['integer'])
        )
        self.assertEqual(
            value_descriptors[1],
            ValueDescriptor("height", datatype.registry['real'])
        )
        self.assertEqual(
            value_descriptors[2],
            ValueDescriptor("state", datatype.registry['text'])
        )
        self.assertEqual(
            value_descriptors[3],
            ValueDescriptor("remark", datatype.registry['smallint'])
        )
    
    def test_deduce_data_types_array(self):
        """The max data types should be deduced from the package."""
        data_package = DataPackage(
            ["curve"],
            [
                (123001, TIMESTAMP, ('0,1,2,4,7,4,2,1,0',)),
                (123002, TIMESTAMP, ('0,1,2,5,8,4,2,1,0',)),
                (123003, TIMESTAMP, ('0,1,3,5,7,4,3,1,0',)),
                (123004, TIMESTAMP, ('0,1,2,4,9,4,2,1,0',))
            ]
        )
    
        data_types = data_package.deduce_value_descriptors()
    
        attr_type_dict = dict(zip(data_package.attribute_names, data_types))
    
        self.assertEqual(
            attr_type_dict["curve"],
            ValueDescriptor('curve', datatype.registry['text'])
        )
    
    def test_deduce_data_types_empty(self):
        data_package = DataPackage(
            attribute_names=('height', 'power', 'refs'),
            rows=[]
        )
    
        value_descriptors = data_package.deduce_value_descriptors()
    
        self.assertEqual(
            value_descriptors[0], ValueDescriptor('height', datatype.registry[
                'smallint'])
        )
    
    def test_to_dict(self):
        data_package = DataPackage(
            attribute_names=('height', 'power'),
            rows=[
                (10034, TIMESTAMP, ['15.6', '68'])
            ]
        )
    
        json_data = data_package.to_dict()
    
        expected_json = {
            "attribute_names": ["height", "power"],
            "rows": [
                [10034, "2013-08-30T15:30:00+00:00", ["15.6", "68"]]
            ]
        }
    
        self.assertEqual(json_data, expected_json)
     
    def test_from_dict(self):
        json_data = {
            "timestamp": "2013-09-16T16:55:00+00:00",
            "attribute_names": ["tilt", "azimuth"],
            "rows": [
                [13403, ["4", "180"]]
            ]
        }
    
        data_package = DataPackage.from_dict(json_data)
    
        self.assertEqual(data_package.attribute_names[1], "azimuth")
        self.assertEqual(data_package.rows[0][0], 13403)
        self.assertEqual(data_package.rows[0][1][1], "180")
    
    def test_deduce_attributes(self):
        data_package = simple_package
    
        attributes = data_package.deduce_attributes()
    
        attr_dict = {
            attribute.name: attribute
            for attribute in attributes
        }
    
        self.assertEqual(
            attr_dict["power"].data_type, datatype.registry['integer']
        )
        self.assertEqual(
            attr_dict["height"].data_type, datatype.registry['real']
        )
        self.assertEqual(
            attr_dict["state"].data_type, datatype.registry['text']
        )
    
    def test_create_copy_from_lines(self):
        """The format of the copy-from-file should be acceptable by PostgreSQL."""
        data_package = DataPackage(
            ["power", "height", "state", "remark"],
            [
                (123001, TIMESTAMP, (405, 0.0, True, "")),
                (123003, TIMESTAMP, (41033, 22.3, True, "")),
                (123004, TIMESTAMP, (880, 30.0, True, ""))
            ]
        )
    
        output_descriptors = [
            OutputDescriptor(
                ValueDescriptor('power', datatype.registry['integer']),
                datatype.copy_from_serializer_config(
                    datatype.registry['integer']
                )
            ),
            OutputDescriptor(
                ValueDescriptor('height', datatype.registry['real']),
                datatype.copy_from_serializer_config(
                    datatype.registry['real']
                )
            ),
            OutputDescriptor(
                ValueDescriptor('state', datatype.registry['boolean']),
                datatype.copy_from_serializer_config(
                    datatype.registry['boolean']
                )
            ),
            OutputDescriptor(
                ValueDescriptor('remark', datatype.registry['text']),
                datatype.copy_from_serializer_config(
                    datatype.registry['text']
                )
            )
        ]
    
        lines = data_package._create_copy_from_lines(output_descriptors)
    
        self.assertEqual(
            lines[0],
            "123001\t2013-08-30 15:30:00+00:00\t405\t0.0\ttrue\t\n"
        )
    
        data_package = DataPackage(
            ["curve"],
            [
                (123001, TIMESTAMP, ([0, 1, 2, 4, 7, 4, 2, 1, 0],)),
                (123002, TIMESTAMP, ([0, 1, 2, 5, 8, 4, 2, 1, 0],)),
                (123003, TIMESTAMP, ([0, 1, 3, 5, 7, 4, 3, 1, 0],)),
                (123004, TIMESTAMP, ([0, 1, 2, 4, 9, 4, 2, 1, 0],))
            ]
        )
    
        output_descriptors = [
            OutputDescriptor(
                ValueDescriptor(
                    data_package.attribute_names[0],
                    datatype.registry['smallint[]'],
                ),
                datatype.copy_from_serializer_config(
                    datatype.registry['smallint[]']
                )
            )
        ]
        lines = data_package._create_copy_from_lines(output_descriptors)
    
        self.assertEqual(
            lines[0],
            "123001\t2013-08-30 15:30:00+00:00\t{0,1,2,4,7,4,2,1,0}\n"
        )
    
        data_package = DataPackage(
            ["curve"],
            [
                (123001, TIMESTAMP, ([0, 1, 2],)),
                (123002, TIMESTAMP, ([0, 1, 2],)),
                (123003, TIMESTAMP, ([None, None, None],))
            ]
        )
    
        serializer_config = datatype.copy_from_serializer_config(
            datatype.registry['smallint[]']
        )
    
        output_descriptors = [
            OutputDescriptor(
                ValueDescriptor('curve', datatype.registry['smallint[]']),
                serializer_config
            )
        ]
    
        lines = data_package._create_copy_from_lines(output_descriptors)
    
        self.assertEqual(
            lines[1], "123002\t2013-08-30 15:30:00+00:00\t{0,1,2}\n"
        )
        self.assertEqual(
            lines[2], "123003\t2013-08-30 15:30:00+00:00\t{\\N,\\N,\\N}\n"
        )
    
        data_package = DataPackage(
            ["curve"],
            [
                (123001, TIMESTAMP, ([None, None],)),
                (123002, TIMESTAMP, ([None, None],))
            ]
        )
    
        output_descriptors = [
            OutputDescriptor(
                ValueDescriptor('curve', datatype.registry['smallint[]']),
                datatype.copy_from_serializer_config(
                    datatype.registry['smallint[]']
                )
            )
        ]
    
        lines = data_package._create_copy_from_lines(output_descriptors)
    
        self.assertEqual(
            lines[0], "123001\t2013-08-30 15:30:00+00:00\t{\\N,\\N}\n"
        )
        self.assertEqual(
            lines[1], "123002\t2013-08-30 15:30:00+00:00\t{\\N,\\N}\n"
        )
