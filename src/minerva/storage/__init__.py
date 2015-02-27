"""
Provides access and a location for storage class logic like 'trend',
'attribute', etc..
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class Engine():
    @staticmethod
    def store(package):
        """
        Returns function that executes a storage class specific store method.
        :param package:
        :return: function(data_source) -> function(conn)
        """
        raise NotImplementedError()
