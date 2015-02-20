# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import StringIO

from minerva.xmlschemaparser.schemabuilder import SchemaBuilder
#from minerva.xmlschemaparser.schematypes import *
from minerva.xmlschemaparser.schematypes import Element, ComplexType, \
    Sequence, SimpleType, Attribute


def test_emptyschema():
    schemabuilder = SchemaBuilder()

    targetnamespace = "firsttest.org"

    xsd = """<schema targetNamespace="%s" xmlns="http://www.w3.org/2001/XMLSchema">
</schema>""" % targetnamespace

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    assert schema.targetnamespace == targetnamespace


def test_simpleelement():
    schemabuilder = SchemaBuilder()

    elementname = "FirstElement"
    elementtype = "string"
    minoccurs = "1"
    maxoccurs = "unbounded"

    xsd = """<?xml version="1.0" encoding="utf-8"?>
<schema targetNamespace="oneelement.org" xmlns="http://www.w3.org/2001/XMLSchema">
<element name="%s" type="%s" minOccurs="%s" maxOccurs="%s"/>
</schema>""" % (elementname, elementtype, minoccurs, maxoccurs)

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    # Schema should have 1 child
    assert len(schema.get_children()) == 1

    child = schema.get_children()[0]

    # The child should be an Element instance
    assert child.__class__ == Element

    # The name of the element should be equal to elementname
    assert child.name == elementname

    element = child

    assert element.typename.localname == elementtype
    assert element.minoccurs == 1
    assert element.maxoccurs == "unbounded"


def test_complexelement():
    """Tests an element with a complex type definition that contains a
    sequence of 5 elements."""
    schemabuilder = SchemaBuilder()

    elementname = "SomeComplexElement"

    xsd = """<?xml version="1.0" encoding="utf-8"?>
<schema targetNamespace="complexelement.org" xmlns="http://www.w3.org/2001/XMLSchema">
<element name="%s">
    <complexType>
    <sequence>
        <element name="First" type="string" minOccurs="0" />
        <element name="Second" type="string" minOccurs="0" />
        <element name="Third" type="string" minOccurs="0" />
        <element name="Fourth" type="string" minOccurs="0" />
        <element name="Fifth" type="string" minOccurs="0" />
    </sequence>
    </complexType>
</element>
</schema>""" % elementname

    stream = StringIO.StringIO(xsd)
    schema = schemabuilder.build_schema(stream)

    # The first child should be the complex element
    complexelement = schema.get_children()[0]

    # The name of the element should be equal to elementname
    assert complexelement.name == elementname

    # The element should have one child: a complex type
    complextype = complexelement.get_children()[0]

    assert complextype.__class__ == ComplexType

    # The complex type should have one child: a sequence
    sequence = complextype.get_children()[0]

    assert sequence.__class__ == Sequence

    assert len(sequence.get_children()) == 5

    assert sequence.get_children()[0].name == "First"
    assert sequence.get_children()[4].name == "Fifth"


def test_named_simpletype():
    schemabuilder = SchemaBuilder()

    typename = "FirstSimpleType"

    xsd = """<schema targetNamespace="oneelement.org" xmlns="http://www.w3.org/2001/XMLSchema">
<simpleType name="%s"/>
</schema>""" % typename

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    # Schema should have 1 child
    assert len(schema.get_children()) == 1

    child = schema.get_children()[0]

    # The child should be a SimpleType instance
    assert child.__class__ == SimpleType

    # The name of the type should equal to typename
    assert child.name == typename


def test_elementref_prefix_translation():
    namespace_name = "testnamespace.org"
    namespace_prefix = "tn"

    schemabuilder = SchemaBuilder()

    xsd = """<?xml version="1.0" encoding="utf-8"?>
<schema targetNamespace="%(NamespaceName)s" xmlns="http://www.w3.org/2001/XMLSchema" xmlns:%(NamespacePrefix)s="%(NamespaceName)s">
<element name="object">
    <complexType>
        <sequence>
            <element name="name" type="string" />
            <element name="id" type="string" />
        </sequence>
    </complexType>
</element>
<element name="encapsulate">
    <complexType>
        <sequence>
            <element ref="tn:object" />
        </sequence>
    </complexType>
</element>
</schema>""" % {"NamespaceName": namespace_name, "NamespacePrefix": namespace_prefix}

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    # Get the element we need for the test
    elementreference = schema.get_children()[1].complextype.get_children()[0].get_children()[0]

    # Check if the reference prefix has been translated to a full namespace URI
    assert elementreference.ref.namespacename == namespace_name


def test_named_complextype():
    schemabuilder = SchemaBuilder()

    typename = "FirstComplexType"

    attributename = "id"
    attributetype = "string"
    attributeuse = "required"

    xsd = """<?xml version="1.0" encoding="utf-8"?>
<schema targetNamespace="oneelement.org" xmlns="http://www.w3.org/2001/XMLSchema">
<complexType name="%s">
    <attribute name="%s" type="%s" use="%s"/>
</complexType>
</schema>""" % (typename, attributename, attributetype, attributeuse)

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    # Schema should have 1 child
    assert len(schema.get_children()) == 1

    child = schema.get_children()[0]

    # The child should be a ComplexType instance
    assert child.__class__ == ComplexType

    complextype = child

    # The name of the type should be equal to typename
    assert child.name == typename

    # The ComplexType should have one child
    assert len(complextype.get_children()) == 1

    complextypechild = complextype.get_children()[0]

    # The child of the ComplexType should be an Attribute instance
    assert complextypechild.__class__ == Attribute

    attribute = complextypechild

    # The name of the attribute should be equal to attributename
    assert attribute.name == attributename

    # The type of the attribute should be equal to attributetype
    assert attribute.type == attributetype

    # The use of this attribute should be equal to attributeuse
    assert attribute.use == attributeuse


def test_real_world_snippet():
    schemabuilder = SchemaBuilder()

    xsd = """<?xml version="1.0" encoding="utf-8"?>
<schema targetNamespace="oneelement.org" xmlns="http://www.w3.org/2001/XMLSchema">
<complexType name="NrmClassXmlType" abstract="true">
    <attribute name="id" type="string" use="required"/>
    <attribute name="modifier" use="optional">
        <simpleType>
            <restriction base="string">
                <enumeration value="create"/>
                <enumeration value="delete"/>
                <enumeration value="update"/>
            </restriction>
        </simpleType>
    </attribute>
</complexType>
<element name="ExternalGsmCell">
    <complexType>
        <complexContent>
            <extension base="oneelement.org:NrmClassXmlType">
                <sequence>
                    <element name="attributes" minOccurs="0">
                        <complexType>
                            <all>
                                <element name="userLabel" minOccurs="0"/>
                                <element name="cellIdentity" minOccurs="0"/>
                            </all>
                        </complexType>
                    </element>
                </sequence>
            </extension>
        </complexContent>
    </complexType>
</element>
</schema>"""

    stream = StringIO.StringIO(xsd)

    schema = schemabuilder.build_schema(stream)

    externalgsmcell = schema.get_children()[1]

    userlabel_element = externalgsmcell.get_children()[0].get_children()[0].get_children()[0].get_children()[0].get_children()[0].get_children()[0].get_children()[0].get_children()[0]

    assert userlabel_element.name == "userLabel"
