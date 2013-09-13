# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class ElementRelation(object):
    """Defines a relation between an element type and an element which can
    be resolved once all element types are defined."""

    def __init__(self, elementtype, namespace_uri, elementpath):
        self.elementtype = elementtype
        self.namespace_uri = namespace_uri
        self.elementpath = elementpath


class BaseTypeRelation(object):

    def __init__(self, elementtype, namespace_uri, typename):
        self.elementtype = elementtype
        self.namespace_uri = namespace_uri
        self.typename = typename


class XmlNamespace(object):

    def __init__(self, name):
        self.name = name
        self.root_elementhandlers = {}
        self.named_types = {}
        self.elementrelations = []
        self.basetyperelations = []

    def add_named_type(self, xmlelementtype):
        self.named_types[xmlelementtype.name] = xmlelementtype
        xmlelementtype.namespace = self

    def get_elementhandler(self, namepath):
        """Returns an element handler from a path like:
        /mdc/md/neid"""
        path = namepath.lstrip('/').split('/')

        root_handler_name = path.pop(0)
        current_handler = self.root_elementhandlers.get(root_handler_name, None)

        while current_handler and len(path) > 0:
            handler_name = path.pop(0)
            current_handler = current_handler.elementtype.get_child_elementhandler(handler_name)

        return current_handler
