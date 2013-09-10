# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import assert_raises, assert_true, assert_false, assert_equal

from minerva.directory.distinguishedname import explode, splitparts, escape


def test_splitparts():
    """
    Check that distinguished names are split correctly
    """
    dnparts = splitparts("SubNetwork=NL1_R,SubNetwork=AHPTUR1,MeContext=AHPTUR1,ManagedElement=1,RncFunction=1,UeRc=9")
    assert_equal(dnparts[0], "SubNetwork=NL1_R")
    assert_equal(len(dnparts), 6)
    assert_equal(dnparts[5], "UeRc=9")

    dnparts = splitparts("Word=asdf,Writer=qwerty\\,dvorak,Reader=Unicode")
    assert_equal(dnparts[0], "Word=asdf")
    assert_equal(len(dnparts), 3)
    assert_equal(dnparts[2], "Reader=Unicode")

def test_escape():
    """
    Check that ',' is escaped correctly
    """
    assert_equal(escape("Word=asdf,fdsa"), "Word=asdf\\,fdsa")
