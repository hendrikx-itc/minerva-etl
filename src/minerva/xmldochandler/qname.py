# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class QName(object):
    """
    A qualified name that contains a namespace name or prefix and a local name.
    """

    def __init__(self, namespacename, localname):
        self.namespacename = namespacename
        self.localname = localname

    @staticmethod
    def split(fullname):
        """
        Split a full name containing a namespace and local name part and return
        them in a tuple.
        """
        colonindex = fullname.find(u':')

        if colonindex >= 0:
            return (fullname[:colonindex], fullname[colonindex+1:])
        else:
            return (None, fullname)

    def __str__(self):
        if self.namespacename != None:
            return "{0:s}:{1:s}".format(self.namespacename, self.localname)
        else:
            return str(self.localname)

    def __eq__(self, other):
        if not isinstance(other, QName):
            return False

        equal = (self.namespacename == other.namespacename and self.localname == other.localname)

        return equal

    def __hash__(self):
        return self.__str__().__hash__()
