import subprocess
import unittest
import tempfile
import json

import docker

from minerva.test.integration import start_db_container

client = docker.from_env()
db_container = None


def setUpModule():
    print('Setup')
    global db_container
    db_container = start_db_container(client)


def tearDownModule():
    print('Teardown')
    global db_container
    db_container.stop()


trend_store_json = {
    'data_source': 'test',
    'entity_type': 'Cell',
    'granularity': '1 day',
    'partition_size': '86400s',
    'parts': [
        {
            'name': 'first_part',
            'trends': [
            ]
        },
        {
            'name': 'second_part',
            'trends': [
            ]
        }
    ]
}


class TestLoadData(unittest.TestCase):
    def test_create_data_source(self):
        proc = subprocess.run(['minerva', 'data-source', 'create', 'test'])

        self.assertEqual(proc.returncode, 0)

        with tempfile.NamedTemporaryFile('wt') as json_tmp_file:
            json.dump(trend_store_json, json_tmp_file)
            json_tmp_file.flush()

            proc = subprocess.run(['minerva', 'trend-store', 'create', '--format=json', json_tmp_file.name])

        self.assertEqual(proc.returncode, 0)

        with tempfile.NamedTemporaryFile() as tmp_file:
            proc = subprocess.run(
                ['minerva', 'load-data', '--data-source', 'test', '--plugin', 'csv', tmp_file.name]
            )

        self.assertEqual(proc.returncode, 0)

        proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

        self.assertEqual(proc.returncode, 0)
