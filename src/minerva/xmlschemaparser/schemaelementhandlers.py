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

from minerva.xmlschemaparser.schematypes import Include, Import, Element, ComplexType, SimpleType, \
    Enumeration, Restriction, Attribute, Choice, SimpleContent, Extension, \
    ComplexContent, Sequence, Union, MaxInclusive, MinInclusive, \
    FractionDigits, MaxLength, All, List, Schema


class SchemaContext(object):

    def __init__(self):
        self.schema = None
        self.prefixmappings = {}
        self.elementstack = []
        self.namespaces = {}

    def startschema(self, targetNamespace):
        self.schema = Schema()
        # self.schema.settargetnamespace(targetNamespace)
        self.schema.targetnamespace = targetNamespace
        self.schema.prefixmappings = self.prefixmappings
        self.prefixmappings = {}

    def startprefixmapping(self, prefix, uri):
        self.prefixmappings[prefix] = uri

    def startxmlelement(self, element):
        if len(self.elementstack) > 0:
            parent = self.elementstack[-1]
            parent.add_child(element)
            element.parent = parent
        else:
            self.schema.add_child(element)

        self.elementstack.append(element)

    def endelement(self):
        self.elementstack.pop()

    def handleelement(self, element):
        self.startxmlelement(element)

        # Find parent XML Element
        for tmp_element in reversed(self.elementstack[:-1]):
            if isinstance(tmp_element, Element):
                element.xml_element_parent = tmp_element
                break

        if isinstance(element.parent, Schema):
            # If the parent is the Schema element, than this is a 'top level'
            # element
            self.schema.toplevelelements.append(element)

    def start_fractiondigits(self, value):
        self.startxmlelement(FractionDigits(value))

    def start_mininclusive(self, value):
        self.startxmlelement(MinInclusive(value))

    def start_maxinclusive(self, value):
        self.startxmlelement(MaxInclusive(value))

    def start_maxlength(self, value):
        self.startxmlelement(MaxLength(value))

    def start_union(self):
        self.startxmlelement(Union())

    def start_sequence(self):
        self.startxmlelement(Sequence())

    def start_all(self):
        self.startxmlelement(All())

    def start_list(self, itemType=None):
        self.startxmlelement(List(itemType))

    def start_complexcontent(self):
        self.startxmlelement(ComplexContent())

    def start_simplecontent(self):
        simplecontent = SimpleContent()

        parent = self.elementstack[-1]

        if isinstance(parent, ComplexType):
            parent.simplecontent = simplecontent

        self.startxmlelement(simplecontent)

    def start_extension(self, base):
        extension = Extension()
        basetyperef = QName(*QName.split(base))

        if basetyperef.namespacename in self.schema.prefixmappings:
            basetyperef = QName(
                self.schema.prefixmappings[basetyperef.namespacename],
                basetyperef.localname
            )

        extension.basetypereference = basetyperef

        parent = self.elementstack[-1]

        if isinstance(parent, (SimpleContent, ComplexContent)):
            parent.extension = extension

        self.startxmlelement(extension)

    def start_choice(self):
        self.startxmlelement(Choice())

    def start_attribute(self, name, type=None, use=None):
        attribute = Attribute()
        attribute.name = name
        attribute.type = type
        attribute.use = use

        self.startxmlelement(attribute)

    def start_restriction(self, base):
        restriction = Restriction()
        restriction.base = base

        self.startxmlelement(restriction)

    def start_enumeration(self, value):
        self.startxmlelement(Enumeration(value))

    def start_simpletype(self, name=None):
        self.startxmlelement(SimpleType(name))

    def start_complextype(self, name=None, abstract=False):
        complexType = ComplexType()
        complexType.name = name
        complexType.abstract = abstract

        self.startxmlelement(complexType)

    def start_element(
            self, name=None, type=None, ref=None, substitutionGroup=None,
            minOccurs=None, maxOccurs=None):
        element = Element()
        element.name = name

        if ref:
            (namespacename, localname) = QName.split(ref)

            if namespacename in self.schema.prefixmappings:
                elementref = QName(
                    self.schema.prefixmappings[namespacename],
                    localname
                )
            else:
                elementref = QName(namespacename, localname)

            element.ref = elementref

        if type:
            (namespacename, localname) = QName.split(type)

            if namespacename in self.schema.prefixmappings:
                typename = QName(
                    self.schema.prefixmappings[namespacename],
                    localname
                )
            else:
                typename = QName(namespacename, localname)

            element.typename = typename

        if substitutionGroup:
            (namespacename, localname) = QName.split(substitutionGroup)

            if namespacename in self.schema.prefixmappings:
                elementref = QName(
                    self.schema.prefixmappings[namespacename],
                    localname
                )
            else:
                elementref = QName(namespacename, localname)

            element.substitutiongroup = elementref

        if minOccurs:
            try:
                element.minoccurs = int(minOccurs)
            except ValueError:
                element.minoccurs = minOccurs

        if maxOccurs:
            try:
                element.maxoccurs = int(maxOccurs)
            except ValueError:
                element.maxoccurs = maxOccurs

        self.handleelement(element)

    def start_import(self, namespace=None, schemaLocation=None):
        importElement = Import()
        importElement.namespacename = namespace
        importElement.schemaLocation = schemaLocation

        self.startxmlelement(importElement)

    def start_include(self, targetNamespace=None, schemaLocation=None):
        include = Include()
        include.schemaLocation = schemaLocation
        include.targetnamespace = targetNamespace

        self.startxmlelement(include)
