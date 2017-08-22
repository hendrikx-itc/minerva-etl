# -*- coding: utf-8 -*-

from minerva.xmldochandler.xmldochandler import XmlDocHandler
from minerva.xmldochandler.schemacontext import SchemaContext
from minerva.xmldochandler.xmlelementtype import XmlElementType
from minerva.xmldochandler.xmlnamespace import XmlNamespace


def test_constructor():
    xmldochandler = XmlDocHandler()

    assert len(xmldochandler._stack) == 0


def test_startElementNS_handlerootelement():
    schemacontext = SchemaContext()

    directorytype = XmlElementType(None, None)
    directoryhandler = directorytype.create_elementhandler("directory")

    namespace = XmlNamespace("somename.org")
    namespace.root_elementhandlers[directoryhandler.name] = directoryhandler
    schemacontext.namespaces[namespace.name] = namespace

    xmldochandler = XmlDocHandler()
    xmldochandler.schemacontext = schemacontext

    xmldochandler.startElementNS(("somename.org", "directory"), None, None)


def test_prefixmapping():
    """
    The function set to onstartprefixmapping should be called for every
    prefix mapping.
    """
    namespaceuri = "somenamespace.org"
    prefix = "ns"

    class MappingHandler:

        def __init__(self):
            self.prefixmappings = {}

        def startprefixmapping(self, prefix, uri):
            self.prefixmappings[prefix] = uri

    mock_mappinghandler = MappingHandler()

    xmldochandler = XmlDocHandler()
    xmldochandler.onstartprefixmapping = mock_mappinghandler.startprefixmapping
    xmldochandler.startPrefixMapping(prefix, namespaceuri)

    assert mock_mappinghandler.prefixmappings[prefix] == namespaceuri
