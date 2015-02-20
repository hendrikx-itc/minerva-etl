# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util.proxy import Proxy


class XmlElementType():

    class XmlElementHandler():

        def __init__(self, name, elementtype):
            """
            Initialize the element handler, its name and type.
            """
            self.namespace = None
            self.name = name
            self.elementtype = elementtype
            self.on_element_start = None
            self.on_element_end = None
            self.substitutiongroup = None
            self.minoccurs = None
            self.maxoccurs = None

        @staticmethod
        def create_localcontext():
            return None

        def __str__(self):
            return self.name

        def start_element(self, local_context, **kwargs):
            if self.on_element_start:
                self.on_element_start(**kwargs)

        def end_element(self, local_context):
            if self.on_element_end:
                self.on_element_end()

        def characters(self, local_context, content):
            pass

    def __init__(self, name, base):
        self.namespace = None
        self.name = name
        self.base = base
        self.attributes = set()
        self._child_elementhandlers = {}

    def create_elementhandler(self, name):
        """
        Create a new element handler for this specific type of element.
        """
        return XmlElementType.XmlElementHandler(name, self)

    def child_elementhandlers(self):
        return self._child_elementhandlers

    def add_child_elementhandlers(self, *args):
        """
        Add one or multiple element handlers which can be provided as separate
        element handlers or as a sequence of elementhandlers.
        """
        for arg in args:
            if hasattr(arg, '__iter__'):
                for elementhandler in arg:
                    self._child_elementhandlers[elementhandler.name] = elementhandler
            else:
                self._child_elementhandlers[arg.name] = arg

    def get_attributes(self):
        if self.base:
            return self.attributes.union(self.base.get_attributes())
        else:
            return self.attributes

    def get_child_elementhandler(self, name):
        """
        Return the child element handler with the specified name from this
        element type or possibly the base type. If no matching handler is
        found, return None.
        """
        elementhandler = self._child_elementhandlers.get(name, None)

        if not elementhandler and self.base:
            elementhandler = self.base.get_child_elementhandler(name)

        return elementhandler


class XmlElementTypeRef(Proxy):

    def __init__(self, ref):
        # The subject is filled in later, based on the reference
        Proxy.__init__(self, None)
        self.ref = ref
