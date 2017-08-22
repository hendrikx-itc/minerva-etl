# -*- coding: utf-8 -*-
from minerva.xmlschemaparser.schemaelementhandlers import SchemaContext


def test_referenceprefixtranslation():
    schemacontext = SchemaContext()
    schemacontext.prefixmappings = {"tn": "testnamespace.org"}
    schemacontext.startschema("testnamespace.org")

    schemacontext.start_element(ref="tn:someelementname")

    assert schemacontext.elementstack[
        -1].ref.namespacename == "testnamespace.org"


def test_typenameprefixtranslation():
    schemacontext = SchemaContext()
    schemacontext.prefixmappings = {"tn": "testnamespace.org"}
    schemacontext.startschema("testnamespace.org")

    schemacontext.start_element(type="tn:sometypename")

    assert schemacontext.elementstack[
        -1].typename.namespacename == "testnamespace.org"
