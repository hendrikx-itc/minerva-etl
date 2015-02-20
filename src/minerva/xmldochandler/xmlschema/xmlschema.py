# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os.path
import sys

parentPath = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(parentPath)

from xmlnamespace import XmlNamespace

from xmlschema_string import XmlSchema_string


class XmlSchema(XmlNamespace):

    def __init__(self):
        XmlNamespace.__init__(self, u'http://www.w3.org/2001/XMLSchema')

        self.addnamedtype(XmlSchema_string())
