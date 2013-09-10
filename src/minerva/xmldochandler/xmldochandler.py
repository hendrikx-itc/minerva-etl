# -*- coding: utf-8 -*-
"""
Provides the XmlDocHandler class to be used with a SAX parser.
"""
__docformat__ = "restructuredtext en"
__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from xml.sax.handler import ContentHandler
from xmlelementtype import XmlElementType
import logging


class XmlDocHandler(ContentHandler):

    def __init__(self):
        ContentHandler.__init__(self)
        self._stack = []
        self.schemacontext = None
        self.onstartprefixmapping = None

    def startElementNS(self, name, qname, attrs):
        matching_handler = None

        if len(self._stack) > 0:
            elementtype = self._stack[-1][0].elementtype

            matching_handler = elementtype.get_child_elementhandler(name[1])
        else:
            matching_handler = self.schemacontext.get_elementhandler(name[0], name[1])

        if matching_handler:
            attributes = matching_handler.elementtype.get_attributes()

            context = matching_handler.create_localcontext()

            if len(attributes) > 0:
                args = {}

                for attribute_name in attributes:
                    attribute_key = (None, attribute_name)

                    value = attrs.get(attribute_key, None)

                    if value:
                        args[attribute_name] = value

                matching_handler.start_element(context, **args)
            else:
                matching_handler.start_element(context)

            self._stack.append((matching_handler, context))
        else:
            # No matching element handler for the current element.
            # Should not occur often, so performance is not an issue here.
            if len(self._stack) > 0:
                parent_handler, context = self._stack[-1]
                raise Exception("No handler found for element '{0}' under parent \
element handler '{1}'".format(name[1], parent_handler.name))
            else:
                raise Exception("No handler found for root element '{0}'".format(name[1]))

            dummy_elementhandler = XmlElementHandler(name[1], XmlElementType(None, None))

            if len(self._stack) > 0:
                self._stack[-1][0].elementtype.addchildelementhandlers(dummy_elementhandler)

            self._stack.append((dummy_elementhandler, None))

            logging.debug("Start of unregistered handler {0!s}\n{1:s}".format(
                name, "->".join(
                    (str(handler) for handler, context in self._stack))))

    def endElementNS(self, name, qname):
        (handler, context) = self._stack.pop()

        if name[1] != handler.name:
            raise Exception("Handler <-> element mismatch: {0} != {1}".format(
                handler.name, name[1]))

        handler.end_element(context)

    def characters(self, content):
        (handler, context) = self._stack[-1]

        handler.characters(context, content)

    def startPrefixMapping(self, prefix, uri):
        if self.onstartprefixmapping is not None:
            self.onstartprefixmapping(prefix, uri)
