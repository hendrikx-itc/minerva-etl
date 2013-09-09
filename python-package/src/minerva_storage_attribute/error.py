# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class NoRecordError(Exception):
    """
    A rather generic exception to indicate that no record was found.
    """
    def __init__(self):
        super(NoRecordError, self).__init__()
