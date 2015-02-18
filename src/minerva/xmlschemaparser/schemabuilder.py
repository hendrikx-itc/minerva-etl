# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from xml.sax import make_parser
from xml.sax.handler import feature_namespaces
from minerva.xmlschemaparser import schemaelementhandlers
from minerva.xmldochandler.xmldochandler import XmlDocHandler
from minerva.xmldochandler.xmlelementtype import XmlElementType
from minerva.xmldochandler import SchemaContext
from minerva.xmldochandler import xmlnamespace
import logging

SCHEMA_NAMESPACE = u"http://www.w3.org/2001/XMLSchema"


class SchemaBuilder():

    def __init__(self):
        # Create a logger for this class with its name
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_schema(self, stream):
        """
        Builds a schema object from a stream of XML Schema (\*.xsd) data.
        """
        generated_schemacontext = schemaelementhandlers.SchemaContext()

        schemadochandler = XmlDocHandler()
        schemadochandler.onstartprefixmapping = generated_schemacontext.startprefixmapping

        schemacontext = SchemaBuilder.build_schemacontext(
            generated_schemacontext
        )

        schemadochandler.schemacontext = schemacontext

        parser = make_parser()
        parser.setFeature(feature_namespaces, 1)

        parser.setContentHandler(schemadochandler)

        parser.parse(stream)

        return generated_schemacontext.schema

    @staticmethod
    def build_schemacontext(generated_schemacontext):
        """
        Sets up all schema XML element handlers
        """
        namespace = xmlnamespace.XmlNamespace(
            u"http://www.w3.org/2001/XMLSchema"
        )

        schema_type = XmlElementType(None, None)
        schema_type.attributes = set(["targetNamespace"])
        schema_handler = schema_type.create_elementhandler("schema")
        schema_handler.on_element_start = generated_schemacontext.startschema

        namespace.root_elementhandlers[schema_handler.name] = schema_handler

        import_type = XmlElementType(None, None)
        import_type.attributes = set(["namespace", "schemaLocation"])
        import_handler = import_type.create_elementhandler("import")
        import_handler.on_element_start = generated_schemacontext.start_import
        import_handler.on_element_end = generated_schemacontext.endelement

        include_type = XmlElementType(None, None)
        include_type.attributes = set(["targetNamespace", "schemaLocation"])
        include_handler = include_type.create_elementhandler("include")
        include_handler.on_element_start = generated_schemacontext.start_include
        include_handler.on_element_end = generated_schemacontext.endelement

        simpletype_type = XmlElementType(None, None)
        simpletype_type.attributes = set(["name"])
        simpletype_handler = simpletype_type.create_elementhandler("simpleType")
        simpletype_handler.on_element_start = generated_schemacontext.start_simpletype
        simpletype_handler.on_element_end = generated_schemacontext.endelement

        list_type = XmlElementType(None, None)
        list_type.attributes = set(["itemType"])
        list_handler = list_type.create_elementhandler("list")
        list_handler.on_element_start = generated_schemacontext.start_list
        list_handler.on_element_end = generated_schemacontext.endelement

        restriction_type = XmlElementType(None, None)
        restriction_type.attributes = set(["base"])
        restriction_handler = restriction_type.create_elementhandler("restriction")
        restriction_handler.on_element_start = generated_schemacontext.start_restriction
        restriction_handler.on_element_end = generated_schemacontext.endelement

        enumeration_type = XmlElementType(None, None)
        enumeration_type.attributes = set(["value"])
        enumeration_handler = enumeration_type.create_elementhandler("enumeration")
        enumeration_handler.on_element_start = generated_schemacontext.start_enumeration
        enumeration_handler.on_element_end = generated_schemacontext.endelement

        maxlength_type = XmlElementType(None, None)
        maxlength_type.attributes = set(["value"])
        maxlength_handler = maxlength_type.create_elementhandler("maxLength")
        maxlength_handler.on_element_start = generated_schemacontext.start_maxlength
        maxlength_handler.on_element_end = generated_schemacontext.endelement

        mininclusive_type = XmlElementType(None, None)
        mininclusive_type.attributes = set(["value"])
        mininclusive_handler = mininclusive_type.create_elementhandler("minInclusive")
        mininclusive_handler.on_element_start = generated_schemacontext.start_mininclusive
        mininclusive_handler.on_element_end = generated_schemacontext.endelement

        maxinclusive_type = XmlElementType(None, None)
        maxinclusive_type.attributes = set(["value"])
        maxinclusive_handler = maxinclusive_type.create_elementhandler("maxInclusive")
        maxinclusive_handler.on_element_start = generated_schemacontext.start_maxinclusive
        maxinclusive_handler.on_element_end = generated_schemacontext.endelement

        fractiondigits_type = XmlElementType(None, None)
        fractiondigits_handler = fractiondigits_type.create_elementhandler("fractionDigits")
        fractiondigits_handler.on_element_start = generated_schemacontext.start_fractiondigits
        fractiondigits_handler.on_element_end = generated_schemacontext.endelement

        union_type = XmlElementType(None, None)
        union_handler = union_type.create_elementhandler("union")
        union_handler.on_element_start = generated_schemacontext.start_union
        union_handler.on_element_end = generated_schemacontext.endelement

        element_type = XmlElementType(None, None)
        element_type.attributes = set(["name", "ref", "type", "substitutionGroup", "minOccurs", "maxOccurs"])
        element_handler = element_type.create_elementhandler("element")
        element_handler.on_element_start = generated_schemacontext.start_element
        element_handler.on_element_end = generated_schemacontext.endelement

        complextype_type = XmlElementType(None, None)
        complextype_type.attributes = set(["name", "abstract"])
        complextype_handler = complextype_type.create_elementhandler("complexType")
        complextype_handler.on_element_start = generated_schemacontext.start_complextype
        complextype_handler.on_element_end = generated_schemacontext.endelement

        attribute_type = XmlElementType(None, None)
        attribute_type.attributes = set(["name", "type", "use"])
        attribute_handler = attribute_type.create_elementhandler("attribute")
        attribute_handler.on_element_start = generated_schemacontext.start_attribute
        attribute_handler.on_element_end = generated_schemacontext.endelement

        complexcontent_type = XmlElementType(None, None)
        complexcontent_handler = complexcontent_type.create_elementhandler("complexContent")
        complexcontent_handler.on_element_start = generated_schemacontext.start_complexcontent
        complexcontent_handler.on_element_end = generated_schemacontext.endelement

        simplecontent_type = XmlElementType(None, None)
        simplecontent_handler = simplecontent_type.create_elementhandler("simpleContent")
        simplecontent_handler.on_element_start = generated_schemacontext.start_simplecontent
        simplecontent_handler.on_element_end = generated_schemacontext.endelement

        extension_type = XmlElementType(None, None)
        extension_type.attributes = set(["base"])
        extension_handler = extension_type.create_elementhandler("extension")
        extension_handler.on_element_start = generated_schemacontext.start_extension
        extension_handler.on_element_end = generated_schemacontext.endelement

        sequence_type = XmlElementType(None, None)
        sequence_handler = sequence_type.create_elementhandler("sequence")
        sequence_handler.on_element_start = generated_schemacontext.start_sequence
        sequence_handler.on_element_end = generated_schemacontext.endelement

        all_type = XmlElementType(None, None)
        all_handler = all_type.create_elementhandler("all")
        all_handler.on_element_start = generated_schemacontext.start_all
        all_handler.on_element_end = generated_schemacontext.endelement

        choice_type = XmlElementType(None, None)
        choice_handler = choice_type.create_elementhandler("choice")
        choice_handler.on_element_start = generated_schemacontext.start_choice
        choice_handler.on_element_end = generated_schemacontext.endelement

        # Link all tree elements together
        schema_handler.elementtype.add_child_elementhandlers(import_handler, include_handler, simpletype_handler, element_handler, complextype_handler)
        list_handler.elementtype.add_child_elementhandlers(simpletype_handler)
        simpletype_handler.elementtype.add_child_elementhandlers(list_handler, restriction_handler, union_handler)
        restriction_handler.elementtype.add_child_elementhandlers(enumeration_handler, maxlength_handler, mininclusive_handler, maxinclusive_handler, fractiondigits_handler)
        union_handler.elementtype.add_child_elementhandlers(simpletype_handler)
        element_handler.elementtype.add_child_elementhandlers(simpletype_handler, complextype_handler)
        attribute_handler.elementtype.add_child_elementhandlers(simpletype_handler)
        complextype_handler.elementtype.add_child_elementhandlers(attribute_handler, complexcontent_handler, simplecontent_handler, sequence_handler, all_handler, choice_handler)
        complexcontent_handler.elementtype.add_child_elementhandlers(extension_handler)
        simplecontent_handler.elementtype.add_child_elementhandlers(extension_handler)
        extension_handler.elementtype.add_child_elementhandlers(sequence_handler, attribute_handler)
        extension_handler.elementtype.add_child_elementhandlers(sequence_handler, all_handler)
        all_handler.elementtype.add_child_elementhandlers(element_handler)
        sequence_handler.elementtype.add_child_elementhandlers(element_handler, choice_handler)
        choice_handler.elementtype.add_child_elementhandlers(element_handler)

        schemacontext = SchemaContext()

        schemacontext.namespaces[namespace.name] = namespace

        return schemacontext
