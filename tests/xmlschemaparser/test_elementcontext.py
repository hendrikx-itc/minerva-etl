# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.xmlschemaparser.schemaelementhandlers import SchemaContext


def test_referenceprefixtranslation():
    schemacontext = SchemaContext()
    schemacontext.prefixmappings = {"tn": "testnamespace.org"}
    schemacontext.startschema("testnamespace.org")

    schemacontext.start_element(ref="tn:someelementname")

    assert schemacontext.elementstack[-1].ref.namespacename == "testnamespace.org"

def test_typenameprefixtranslation():
    schemacontext = SchemaContext()
    schemacontext.prefixmappings = {"tn": "testnamespace.org"}
    schemacontext.startschema("testnamespace.org")

    schemacontext.start_element(type="tn:sometypename")

    assert schemacontext.elementstack[-1].typename.namespacename == "testnamespace.org"
