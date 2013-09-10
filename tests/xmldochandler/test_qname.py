# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.xmldochandler.qname import QName


def test_constructor():
    namespaceuri = "somename.org"
    localname = "directory"
    qname = QName(namespaceuri, localname)

    assert qname.namespacename == namespaceuri
    assert qname.localname == localname


def test_split():
    parts = QName.split("somename.org:directory")

    assert parts[0] == "somename.org"
    assert parts[1] == "directory"
