from contextlib import closing
import subprocess
import unittest

from nose.tools import eq_

from minerva.test import connect


class MinervaDump(unittest.TestCase):
    """
    Use standard Python unittest TestCase here
    because of the assertMultiLineEqual
    function.
    """
    def test_run(self):
        self.maxDiff = None
        with closing(connect()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute("DELETE FROM trend.trendstore")
                cursor.execute(
                        "DELETE FROM attribute_directory.attributestore")
                cursor.execute(
                    "SELECT trend.create_trendstore("
                    "    'test-datasource',"
                    "    'test-entitytype',"
                    "    '900',"
                    "    ARRAY["
                    "        ('x', 'integer', 'test trend'),"
                    "        ('y', 'double precision', 'another test trend')"
                    "    ]::trend.trend_descr[]"
                    ")")

                cursor.execute(
                    "SELECT attribute_directory.create_attributestore("
                    "    'test-datasource',"
                    "    'test-entitytype',"
                    "    ARRAY["
                    "      ('height','double precision', 'fictive attribute'),"
                    "      ('power', 'integer', 'another fictive attribute')"
                    "    ]::attribute_directory.attribute_descr[]"
                    ")"
                )

            conn.commit()

        process = subprocess.Popen(['minerva-dump'], stdout=subprocess.PIPE)
        out, err = process.communicate()

        self.assertMultiLineEqual(out, """\
SELECT trend.create_trendstore(
    'test-datasource',
    'test-entitytype',
    '900',
    ARRAY[
        ('x', 'integer', ''),
        ('y', 'double precision', '')
    ]::trend.trend_descr[]
);

SELECT attribute_directory.create_attributestore(
    'test-datasource',
    'test-entitytype',
    ARRAY[
        ('height', 'double precision', 'fictive attribute'),
        ('power', 'integer', 'another fictive attribute')
    ]::attribute_directory.attribute_descr[]
);

""")
