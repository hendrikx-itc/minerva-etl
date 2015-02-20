# -*- coding: utf-8 -*-
"""Provides the Struct class."""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class Struct(dict):

    """Wraps a dictionary and makes its values accessible as attributes."""

    def __getattribute__(self, name):
        value = self[name]

        if isinstance(value, dict):
            return Struct(value)
        else:
            return value
