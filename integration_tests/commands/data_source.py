import subprocess
import unittest


class TestDataSource(unittest.TestCase):
    def test_create_data_source(self):
        proc = subprocess.run(['minerva', 'data-source', 'create', 'test'])

        self.assertEqual(proc.returncode, 0)

        proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

        self.assertEqual(proc.returncode, 0)
