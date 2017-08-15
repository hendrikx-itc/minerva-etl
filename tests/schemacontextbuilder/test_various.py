# -*- coding: utf-8 -*-

import sys
import os

from nose.tools import assert_raises, assert_true, assert_false, assert_equal

import minerva
from minerva.xmldochandler.qname import QName
from minerva.xmlschemaparser.schematypes import *
from minerva.schemacontextbuilder.schemacontextbuilder import \
        SchemaContextBuilder


def test_emptyschemawithtargetnamespace():
    namespacename = "testnamespace.org"

    # Build an empty schema
    schema = Schema()
    schema.targetnamespace = namespacename

    schemacontextbuilder = SchemaContextBuilder([schema])

    schemacontext = schemacontextbuilder.build()

    namespace = schemacontext.namespaces.get(namespacename, None)

    assert namespace != None

    assert_equal(namespace.name, namespacename)


def testsimpleelement():
    namespacename = "testnamespace.org"

    # Build a schema with one string element
    schema = Schema()
    schema.targetnamespace = namespacename

    root_element = schema.add_child(Element("book"))
    root_element.typename = QName(namespacename, "string")

    schemacontextbuilder = SchemaContextBuilder([schema])

    schemacontext = schemacontextbuilder.build()

    namespace = schemacontext.namespaces.get(namespacename, None)

    elementhandler = namespace.get_elementhandler("/book")

    assert_equal(elementhandler.name, "book")

    assert_equal(
            elementhandler.elementtype.__class__,
            minerva.xmldochandler.xmlschema.xmlschema_string.XmlSchema_string
            )


def test_complexelement():
    namespacename = "testnamespace.org"

    # Build a schema with one complex element
    schema = Schema()
    schema.targetnamespace = namespacename

    root_element = schema.add_child(Element("book"))

    complextype = root_element.add_child(ComplexType())

    sequence = complextype.add_child(Sequence())

    first_element = sequence.add_child(Element("First"))
    first_element.typename = QName(namespacename, "string")

    second_element = sequence.add_child(Element("Second"))
    second_element.typename = QName(namespacename, "string")

    schemacontextbuilder = SchemaContextBuilder([schema])

    schemacontext = schemacontextbuilder.build()

    namespace = schemacontext.namespaces.get(namespacename, None)

    book_handler = namespace.get_elementhandler("/book")

    assert book_handler.elementtype.get_child_elementhandler("First")

    first_handler = namespace.get_elementhandler("/book/First")
    assert first_handler != None
    secondHandler = namespace.get_elementhandler("/book/Second")
    assert secondHandler != None


def test_elementreference():
    namespacename = "testnamespace.org"

    schema = Schema()
    schema.targetnamespace = namespacename

    managedelement_element = schema.add_child(Element("ManagedElement"))
    managedelement_element.typename = QName(namespacename, "string")

    subnetwork_element = schema.add_child(Element("SubNetwork"))

    complextype = subnetwork_element.add_child(ComplexType())

    sequence = complextype.add_child(Sequence())

    # Define the element reference
    elementreference = sequence.add_child(Element())
    elementreference.ref = QName(namespacename, "ManagedElement")

    schemacontextbuilder = SchemaContextBuilder([schema])

    schemacontext = schemacontextbuilder.build()

    namespace = schemacontext.namespaces.get(namespacename, None)

    subnetworkhandler = namespace.get_elementhandler("/SubNetwork")

    # The referenced child element 'ManagedElement'
    # should be turned into a concrete handler
    managedelementhandler = (
            subnetworkhandler.elementtype.get_child_elementhandler(
                    "ManagedElement"))

    assert managedelementhandler != None

    assert_equal(managedelementhandler.elementtype.name, "string")


def test_extension():
    namespacename = "testnamespace.org"
    abstracttypename = "NrmClassXmlType"

    schema = Schema()
    schema.targetnamespace = namespacename

    # Define an abstract base type
    abstracttype = schema.add_child(ComplexType())
    abstracttype.name = abstracttypename
    abstracttype.abstract = True

    gsmrelationelement = schema.add_child(Element("GsmRelation"))

    complextype = gsmrelationelement.add_child(ComplexType())

    complexcontent = complextype.add_child(ComplexContent())

    extension = complexcontent.add_child(Extension())
    extension.basetypereference = QName(namespacename, abstracttypename)

    sequence = extension.add_child(Sequence())

    element = sequence.add_child(Element("userLabel"))

    schemacontextbuilder = SchemaContextBuilder([schema])

    schemacontext = schemacontextbuilder.build()

    namespace = schemacontext.namespaces.get(namespacename, None)

    gsmrelationhandler = namespace.get_elementhandler("/GsmRelation")

    userlabelelementhandler = (
        gsmrelationhandler.elementtype.get_child_elementhandler(
                    "userLabel"))

    # Type of GsmRelation should have child handler for 'userLabel'
    assert userlabelelementhandler != None

    assert_equal(userlabelelementhandler.elementtype.name, "string")
