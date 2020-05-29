import unittest

from minerva.instance import AttributeStore


class TestAttributeStore(unittest.TestCase):
    def test_from_json(self):
        json_data = {
            'data_source': 'oss-4g',
            'entity_type': 'Cell',
            'attributes': [
                {
                    'name': 'administrative_id',
                    'data_type': 'text'
                },
                {
                    'name': 'band',
                    'data_type': 'text'
                }
            ]
        }

        attribute_store = AttributeStore.from_json(json_data)

        self.assertEqual(attribute_store.data_source, 'oss-4g')
