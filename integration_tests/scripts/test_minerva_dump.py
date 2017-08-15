from contextlib import closing
import subprocess
import unittest

from minerva.test import connect


class MinervaDump(unittest.TestCase):
    """
    Use standard Python unittest TestCase here because of the
    assertMultiLineEqual function.
    """
    def test_run(self):
        self.maxDiff = None
        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute("DELETE FROM trend_directory.trend_store")
                cursor.execute(
                        "DELETE FROM attribute_directory.attribute_store")
                cursor.execute(
                    "SELECT trend_directory.create_table_trend_store("
                    "    'test-data-source',"
                    "    'test-entity-type',"
                    "    '00:15:00',"
                    "    ARRAY["
                    "        ('x', 'integer', 'test trend'),"
                    "        ('y', 'double precision', 'another test trend')"
                    "    ]::trend_directory.trend_descr[]"
                    ")")

                cursor.execute(
                    "SELECT attribute_directory.create_attribute_store("
                    "    'test-data-source',"
                    "    'test-entity-type',"
                    "    ARRAY["
                    "       ('height', 'double precision',"
                    "           'fictive attribute'),"
                    "       ('power', 'integer', 'another fictive attribute')"
                    "    ]::attribute_directory.attribute_descr[]"
                    ")"
                )

            conn.commit()

        process = subprocess.Popen(['minerva-dump'], stdout=subprocess.PIPE)
        out, err = process.communicate()

        self.assertMultiLineEqual(out.decode('utf-8'), """\
SELECT trend_directory.create_table_trend_store(
    'test-data-source',
    'test-entity-type',
    '0:15:00',
    ARRAY[
        ('x', 'integer', 'test trend'),
        ('y', 'double precision', 'another test trend')
    ]::trend_directory.trend_descr[]
);

SELECT attribute_directory.create_attribute_store(
    'test-data-source',
    'test-entity-type',
    ARRAY[
        ('height', 'double precision', 'fictive attribute'),
        ('power', 'integer', 'another fictive attribute')
    ]::attribute_directory.attribute_descr[]
);

""")
