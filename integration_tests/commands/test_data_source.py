import subprocess
import unittest

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


class TestDataSource(unittest.TestCase):
    def test_create_data_source(self):
        proc = subprocess.run(['minerva', 'data-source', 'create', 'test'])

        self.assertEqual(proc.returncode, 0)

        proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

        self.assertEqual(proc.returncode, 0)
