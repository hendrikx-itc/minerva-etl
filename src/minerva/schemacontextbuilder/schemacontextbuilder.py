# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.xmlschemaparser import schematypes
from minerva.xmldochandler import schemacontext
from minerva.xmldochandler.xmlelementtype import XmlElementType
from minerva.xmldochandler.xmlelementtype import XmlElementTypeRef
from minerva.xmldochandler.xmlelementhandler import XmlElementHandlerRef
from minerva.xmldochandler.xmlschema import xmlschema_string


class SchemaBuilderError(Exception):
    pass


class Namespace():

    def __init__(self, uri):
        self.uri = uri

        self.types = {}
        self.unnamedtypes = []
        self.elements = {}

    def add_element(self, element):
        fullname = element.build_fullname()

        if fullname not in self.elements:
            self.elements[fullname] = element

    def get_sorted_elementnames(self):
        return sorted(
            element.build_fullname() for element in self.elements.itervalues()
        )

    def __str__(self):
        if self.uri is not None:
            return self.uri
        else:
            return "Global namespace"

    def shortname(self):
        """
        Tries to extract a sensible short name for the namespace
        """
        result = ""

        if self.uri is not None:
            url_elements = urlsplit(self.uri)

            fragment = url_elements[4]

            if len(fragment) > 0:
                result = fragment
            else:
                result = os.path.split(url_elements[2])[1]

            result = result.replace(".", "_")
            result = result.replace("-", "_")
            result = result.replace(":", "_")

        return result


def walk(element, depth=0):
    yield (element, depth)

    for child_element in element.get_children():
        for (e, d) in walk(child_element, depth + 1):
            yield (e, d)


class SchemaContextBuilder():
    """
    Takes a set of namespaces built by a SchemaBuilder and turns them into an
    XML SchemaContext that can be used for parsing XML documents
    """

    def __init__(self, schemas):
        self.schemas = schemas
        self.xsd_namespaces = {}
        self.schemacontext = None
        self.current_xsd_namespace = None
        self.complextypes = None

    def build(self):
        for schema in self.schemas:
            namespace = self.xsd_namespaces.get(schema.targetnamespace, None)

            if not schema.targetnamespace in self.xsd_namespaces:
                namespace = Namespace(schema.targetnamespace)
                self.xsd_namespaces[namespace.uri] = namespace

            for (element, depth) in walk(schema):
                if isinstance(element, schematypes.Element):
                    namespace.add_element(element)
                elif element.__class__ in set([schematypes.ComplexType]) and element.name:
                    namespace.types[element.name] = element

        self.schemacontext = schemacontext.SchemaContext()

        for (name, namespace) in self.xsd_namespaces.iteritems():
            if namespace.uri != u'http://www.w3.org/2001/XMLSchema':
                self.current_xsd_namespace = namespace
                self.xmlnamespace = self.schemacontext.get_namespace(
                    str(self.current_xsd_namespace)
                )
                self.complextypes = {}

                self.create_named_types()
                self.create_elementhandlers()

        self.schemacontext.link_basetypes()
        self.schemacontext.link_substitutions()
        self.schemacontext.link_handlers()
        self.schemacontext.link_elementreferences()

        return self.schemacontext

    def create_named_types(self):
        for (name, type) in self.current_xsd_namespace.types.iteritems():
            elementtype = None

            if isinstance(type, schematypes.SimpleType):
                elementtype = XmlElementType(name, None)

                if type.restriction:
                    pass
            elif isinstance(type, schematypes.ComplexType):
                elementtype = self.build_complextype(type, None)

            if elementtype:
                self.xmlnamespace.add_named_type(elementtype)

    def build_elementhandlers(self, element):
        elementhandlers = []

        for childelement in element.get_children():
            if isinstance(childelement, schematypes.Element):
                elementhandler = self.build_elementhandler(childelement)

                elementhandlers.append(elementhandler)
            else:
                elementhandlers += self.build_elementhandlers(childelement)

        return elementhandlers

    def build_complextype(self, xsdtype, container_element):
        elementtype = None

        if xsdtype.simplecontent and xsdtype.simplecontent.extension:
            basetype = xsdtype.simplecontent.extension.basetypereference.type

            if basetype.schema.target_namespace.uri == "http://www.w3.org/2001/XMLSchema":
                if basetype.name == "string":
                    elementtype = xmlschema_string.XmlSchema_string()
                if basetype.name == "date":
                    elementtype = xmlschema_string.XmlSchema_string()
        else:
            if xsdtype.name != None:
                elementtype = XmlElementType(xsdtype.name, None)
            else:
                elementtype = XmlElementType(None, None)
                self.complextypes[container_element.build_fullname()] = elementtype

        if xsdtype.complexcontent:
            if xsdtype.complexcontent.extension:
                basetypereference = XmlElementTypeRef(
                    xsdtype.complexcontent.extension.basetypereference
                )
                elementtype.base = basetypereference
                self.schemacontext.basetypereferences.append(basetypereference)

        if xsdtype.basetyperef != None:
            basetyperelation = BaseTypeRelation(
                elementtype, str(xsdtype.basetyperef.type.namespace.uri),
                str(xsdtype.basetyperef.type.name)
            )
            self.xmlnamespace.basetyperelations.append(basetyperelation)

        if len(xsdtype.attributes) > 0:
            for attribute in xsdtype.attributes:
                # elementtype.addAttribute(AttributeHandler(attribute.name))
                raise Exception("Attribute support not implemented yet")

        attribute_names = []

        for child in xsdtype.get_children():
            if isinstance(child, schematypes.Attribute):
                attribute = child
                attribute_names.append(str(attribute.name))

        elementtype.attributes = set(attribute_names)

        elementhandlers = self.build_elementhandlers(xsdtype)

        elementtype.add_child_elementhandlers(elementhandlers)

        self.schemacontext.all_types.append(elementtype)

        return elementtype

    def build_elementhandler(self, element):
        if element.typename != None:
            if element.typename.localname == 'string':
                elementtype = xmlschema_string.XmlSchema_string()
                elementhandler = elementtype.create_elementhandler(element.name)
            elif element.typename.localname == 'date':
                elementtype = xmlschema_string.XmlSchema_string()
                elementhandler = elementtype.create_elementhandler(element.name)
            else:
                elementtype = self.schemacontext.get_elementtype(
                    element.typename.namespacename, element.typename.localname
                )

                if not elementtype:
                    raise SchemaBuilderError(
                        "No type found with name {0:s}".format(element.typename)
                    )

                elementhandler = elementtype.create_elementhandler(element.name)
        else:
            children = element.get_children()

            if len(children) and isinstance(children[0], schematypes.ComplexType):
                elementtype = self.build_complextype(children[0], element)
                elementhandler = elementtype.create_elementhandler(element.name)

            elif element.ref != None:
                elementhandler = XmlElementHandlerRef(element.ref)

                self.schemacontext.elementreferences.append(elementhandler)
            else:
                elementtype = xmlschema_string.XmlSchema_string()
                elementhandler = elementtype.create_elementhandler(element.name)

        if element.substitutiongroup:
            elementhandler.substitutiongroup = element.substitutiongroup
            self.schemacontext.substitutions.append(
                schemacontext.Substitution(
                    elementhandler, element.substitutiongroup
                )
            )

        elementhandler.minoccurs = element.minoccurs
        elementhandler.maxoccurs = element.maxoccurs

        return elementhandler

    def create_elementhandlers(self):
        root_elementhandler_generator = (
            self.build_elementhandler(element)
            for element in self.current_xsd_namespace.elements.values()
            if not element.xml_element_parent
        )

        for elementhandler in root_elementhandler_generator:
            self.xmlnamespace.root_elementhandlers[elementhandler.name] = elementhandler
            namespace = self.schemacontext.namespaces[self.current_xsd_namespace.uri]
            namespace.root_elementhandlers[elementhandler.name] = elementhandler
