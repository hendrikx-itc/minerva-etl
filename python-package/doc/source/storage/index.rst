:mod:`minerva.storage` --- Storage Module
=========================================
While most storage functionality is delegated to plugins, some functionality is
generic enough to make it available in a central location.

Concepts
--------

Raw Data Package
^^^^^^^^^^^^^^^^

This is the format of the data as it will be turned over from any parsing or processing code
to the Minerva code that will refine and store it. A raw data package is just a tuple with the
following structure:

+-------+--------------+-----------------------------------------------------------------------+
| Index | Name \(1)    | Values                                                                |
+=======+==============+=======================================================================+
| 0     | SpawnTime    | A string describing a date and time with format ``%Y-%m-%dT%H:%M:%S`` |
|       |              | where the time is in UTC.                                             |
+-------+--------------+-----------------------------------------------------------------------+
| 1     | IntervalSize | An integer value with range[0,max_int] that tells the size of the     |
|       |              | interval in seconds over which the trend values have meaning.         |
+-------+--------------+-----------------------------------------------------------------------+
| 2     | TrendNames   | A list with all names of the trends that the packages contains        |
|       |              | values of. Each name is a basic Python string.                        |
+-------+--------------+-----------------------------------------------------------------------+
| 3     | ValueTable   | A list with distinguished names and values (see below).               |
+-------+--------------+-----------------------------------------------------------------------+

(1)
    These names are not included in the raw data package, but are used as a means of referring
    to them.

ValueTable

This is the structure inside a raw data packages that contains the real data. A ValueTable
consists of a sequence of tuples ``(distinguished_name, values)`` where ``values`` in turn is a
sequence of strings with the trend values.

Data Package
^^^^^^^^^^^^

A refined form of the data from a raw data package.

``(spawn_time, trend_group, trends, value_table)``

value_table is a sequence of tuples ``(entity, values)``

Available Types
---------------

.. automodule:: minerva.storage
    :members:
    :undoc-members:
