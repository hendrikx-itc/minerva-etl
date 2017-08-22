# -*- coding: utf-8 -*-
from nose.tools import assert_raises, assert_true, assert_false, assert_equal

from minerva.util.tabulate import render_table

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


def test_render_table():
    """
    Check for correct table rendering
    """
